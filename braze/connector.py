"""Currents connector writers, each reading its own connector topic at its partner's pace.

One shard of the connector fleet: it owns the connector topics whose crc32(name) % --shards ==
--shard and reads each of their partitions directly (its own offsets, no consumer group, so no
rebalances), in turn, on --readers connections. Each connector has a token bucket for its
partner's rate limit (--rate-limit events a second). One connector's partner can go down
(--outage, --outage-at, --outage-for): that connector stops reading, its offset stays where it
was, and its events wait on its own topic; when the partner is back it drains from that offset
at the rate limit. Every other connector on the shard is read as if nothing happened.

For each event it drops repeats (per-user sequence; delivery is at least once, as in Currents),
counts any gap in the sequence as a lost event, and times it. Prints a line a second:
  * event-to-connector latency for healthy connectors, PASS or FAIL against --budget-ms;
  * how many healthy connectors had any event over budget this second;
  * events lost so far (sequence gaps) and repeats dropped;
  * any connector whose partner is down or at its rate limit: backlog on its own topic, oldest wait;
  * when that connector has caught up, how long it took and how many events were lost.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, STREAM, Window, connect, connectors, load, shard_of

BANK = 0.5                                                # seconds of rate limit a partition may bank for a burst


async def main(a):
    name = f"conn-{a.shard}"
    admin = await connect(a.url)
    if a.apps:                                            # the kit's own connectors, from router.py
        mine = {t: parts for t, (_, _, parts) in connectors(a.apps).items() if shard_of(t, a.shards) == a.shard}
        while not all([await admin.get_topic(STREAM, t) for t in mine]):
            print(f"[{name}] waiting for router.py to create the connector topics…", flush=True)
            await asyncio.sleep(1)
    else:                                                 # every connector topic in the stream, e.g. from kafka_bridge.py
        while not await admin.get_stream(STREAM):
            await asyncio.sleep(1)
        mine = {t.name: t.partitions_count for t in await admin.get_topics(STREAM)
                if shard_of(t.name, a.shards) == a.shard}
    plist = [(t, pid) for t, parts in mine.items() for pid in range(parts)]
    per_part = {(t, pid): a.rate_limit / mine[t] for t, pid in plist}   # events/s per partition
    tokens = {k: per_part[k] * BANK for k in plist}
    refilled = {k: time.monotonic() for k in plist}
    nxt = {}                                              # next offset per partition: the connector's own position
    held_at, why = {}, {}                                 # connector -> when last held (ns), and why
    last_seq = {}
    budget_ns = a.budget_ms * 1_000_000
    lat, held_wait = Window(), defaultdict(float)
    st, total = defaultdict(int), defaultdict(int)
    late, healthy_read = set(), set()
    caught_up = [False]                                   # a new reader first replays what was written before it
    drain = {}                                            # connector -> when its trouble began, for the catch-up report
    t0 = time.monotonic()
    out_from, out_to = a.outage_at, a.outage_at + a.outage_for
    outage = a.outage if a.outage in mine and a.outage_for > 0 else None
    gc.freeze()                                           # keep startup tables out of the collector's way

    def is_down(t):
        return t == outage and out_from <= time.monotonic() - t0 < out_to

    def apply(t, ms, now):
        held = now - held_at.get(t, 0) < 2e9              # partner down or at its limit in the last 2 s
        for m in ms:
            e = load(m.payload())
            k = (t, e["u"])
            last = last_seq.get(k, 0)
            if e["seq"] <= last:
                st["repeats"] += 1                        # a repeat: export it once
                continue
            if e["seq"] > last + 1:
                total["lost"] += e["seq"] - last - 1      # a gap in this user's sequence: events missing
                total["lost_" + t] += e["seq"] - last - 1
            last_seq[k] = e["seq"]
            age = now - e["ns"]
            if held:
                st["held_" + t] += 1
                held_wait[t] = max(held_wait[t], age / 1e9)
            elif not caught_up[0]:
                st["replayed"] += 1                       # written before this shard started: not live latency
                st["max_age"] = max(st["max_age"], age)
            else:
                lat.add(age / 1e6)
                healthy_read.add(t)
                if age > budget_ns:
                    late.add(t)
            st["exported"] += 1

    async def reader(parts):
        c = await connect(a.url)
        consumers = {k: Consumer.Single(f"{name}-{k[0]}-{k[1]}") for k in parts}
        while True:
            more = False
            for k in parts:
                t = k[0]
                if is_down(t):                            # the partner is down: leave the events on the topic
                    held_at[t], why[t] = time.time_ns(), "down"
                    drain.setdefault(t, time.monotonic())
                    continue
                now = time.monotonic()
                tokens[k] = min(per_part[k] * BANK, tokens[k] + (now - refilled[k]) * per_part[k])
                refilled[k] = now
                allow = min(a.batch, int(tokens[k]))
                if allow < 1:
                    continue
                start = PollingStrategy.Offset(nxt[k]) if k in nxt else PollingStrategy.First()
                ms = await c.poll_messages(STREAM, t, consumer=consumers[k], partition_id=k[1],
                                           count=allow, auto_commit=False, polling_strategy=start)
                if ms:
                    tokens[k] -= len(ms)
                    nxt[k] = ms[-1].offset() + 1
                    if len(ms) == allow < a.batch:        # the rate limit, not the topic, ended this read:
                        held_at[t], why[t] = time.time_ns(), "limit"   # the rest waits on its own topic
                        drain.setdefault(t, time.monotonic())
                    apply(t, ms, time.time_ns())
                    more = more or len(ms) == a.batch     # a full batch inside the limit: read on at once
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def backlog(t):
        tp = await admin.get_topic(STREAM, t)
        # every message in the topic minus every one this connector has read (it reads from the first offset)
        return max(0, tp.messages_count - sum(nxt.get((t, pid), 0) for pid in range(mine[t]))) if tp else 0

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the replay is finished
                if st["replayed"] and st["max_age"] < budget_ns:
                    caught_up[0] = True
                print(f"[{name}] catching up: replayed {st['replayed']:,} earlier events this second", flush=True)
                st.clear()
                lat.flush()
                continue
            r = lat.flush()
            troubled = sum(1 for t in mine if time.time_ns() - held_at.get(t, 0) < 2e9)
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{name}] {r['n']:>6,} events/s exported by {len(healthy_read)} connectors  event-to-connector "
                  f"p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"behind {len(late)}/{len(mine) - troubled} healthy connectors  lost {total['lost']:,}  "
                  f"repeats dropped {st['repeats']:,}", flush=True)
            for t in sorted(drain):
                b = await backlog(t)
                if why.get(t) == "down" and is_down(t):
                    print(f"[{name}] partner down: {t} read 0/s  backlog {b:,} on its own topic, offset held  "
                          f"down for {time.monotonic() - drain[t]:4.1f} s", flush=True)
                elif b > a.batch or (time.time_ns() - held_at.get(t, 0) < 2e9 and st["held_" + t]):
                    print(f"[{name}] at partner rate limit: {t} read {st['held_' + t]:,}/s (limit "
                          f"{a.rate_limit:,.0f}/s)  backlog {b:,} on its own topic  oldest waiting {held_wait[t]:5.1f} s",
                          flush=True)
                else:
                    print(f"[{name}] {t} caught up: backlog cleared {time.monotonic() - drain[t]:.1f} s after its "
                          f"trouble began, {total['lost_' + t]:,} events lost", flush=True)
                    del drain[t]
                held_wait[t] = 0.0
            st.clear()
            late.clear()
            healthy_read.clear()

    groups = [plist[r::a.readers] for r in range(a.readers)]
    print(f"[{name}] reading {len(mine)} connectors, {len(plist)} partitions, on {a.readers} connections", flush=True)
    await asyncio.gather(report(), *(reader(g) for g in groups if g))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--shard", type=int, default=0)
    p.add_argument("--shards", type=int, default=1, help="connector processes splitting the connectors")
    p.add_argument("--apps", type=int, default=150, help="same as router.py; 0 = every connector topic in the stream")
    p.add_argument("--readers", type=int, default=2, help="connections reading this shard's partitions")
    p.add_argument("--rate-limit", type=float, default=5_000, help="events a second each connector's partner accepts")
    p.add_argument("--outage", default="app-0002-storage", help="connector whose partner goes down")
    p.add_argument("--outage-at", type=float, default=60, help="seconds after start the partner goes down")
    p.add_argument("--outage-for", type=float, default=0, help="outage length in seconds (0 = none)")
    p.add_argument("--budget-ms", type=float, default=100, help="p99 budget, event to connector, healthy connectors")
    p.add_argument("--poll-ms", type=float, default=5, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
