"""Data Connector workers, each reading its own stream subscriptions at their destination's pace.

One shard of the connector fleet: it owns the subscription topics whose crc32(name) % --shards ==
--shard and reads each of their partitions directly (its own offsets, no consumer group, so no
rebalances), in turn, on --readers connections. Each subscription has a token bucket for its
destination's write limit (--rate-limit datapoints a second): the customer's own Kafka, MSK,
Event Hubs or Pub/Sub. One customer's broker can go down (--outage, --outage-at, --outage-for):
that subscription stops reading, its offset stays at the last streamed datapoint, and its
datapoints wait on its own topic; when the broker is back it backfills from that offset at the
limit until it has caught up, as Samsara documents for its 6-hour recovery. Every other
subscription on the shard is read as if nothing happened.

For each datapoint it drops repeats (per-vehicle sequence; read at least once), counts any gap in
the sequence as a lost datapoint, and times it. Prints a line a second:
  * datapoint-to-connector latency for healthy subscriptions, PASS or FAIL against --budget-ms;
  * how many healthy subscriptions had any datapoint over budget this second;
  * datapoints lost so far (sequence gaps) and repeats dropped;
  * any subscription whose broker is down or at its limit: backlog on its own topic, oldest wait
    against the 6-hour recovery window;
  * when that subscription has caught up, how long it took and how many datapoints were lost.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, RECOVERY, STREAM, Window, connect, load, shard_of, subscriptions

BANK = 0.5                                                # seconds of rate limit a partition may bank for a burst


async def main(a):
    name = f"conn-{a.shard}"
    admin = await connect(a.url)
    if a.orgs:                                            # the kit's own subscriptions, from datapoints.py
        mine = {t: parts for t, (_, _, parts) in subscriptions(a.orgs).items() if shard_of(t, a.shards) == a.shard}
        while not all([await admin.get_topic(STREAM, t) for t in mine]):
            print(f"[{name}] waiting for datapoints.py to create the subscription topics…", flush=True)
            await asyncio.sleep(1)
    else:                                                 # every subscription topic in the stream, e.g. from kinesis_bridge.py
        while not await admin.get_stream(STREAM):
            await asyncio.sleep(1)
        mine = {t.name: t.partitions_count for t in await admin.get_topics(STREAM)
                if shard_of(t.name, a.shards) == a.shard}
    plist = [(t, pid) for t, parts in mine.items() for pid in range(parts)]
    per_part = {(t, pid): a.rate_limit / mine[t] for t, pid in plist}   # datapoints/s per partition
    tokens = {k: per_part[k] * BANK for k in plist}
    refilled = {k: time.monotonic() for k in plist}
    nxt = {}                                              # next offset per partition: after the last streamed datapoint
    held_at, why = {}, {}                                 # subscription -> when last held (ns), and why
    last_seq = {}
    budget_ns = a.budget_ms * 1_000_000
    lat, held_wait, oldest = Window(), defaultdict(float), defaultdict(float)
    window = RECOVERY.total_seconds()                     # how long a backlog may wait before the log expires it
    st, total = defaultdict(int), defaultdict(int)
    late, healthy_read = set(), set()
    caught_up = [False]                                   # a new reader first replays what was written before it
    drain = {}                                            # subscription -> when its trouble began, for the catch-up report
    t0 = time.monotonic()
    out_from, out_to = a.outage_at, a.outage_at + a.outage_for
    outage = a.outage if a.outage in mine and a.outage_for > 0 else None
    gc.freeze()                                           # keep startup tables out of the collector's way

    def is_down(t):
        return t == outage and out_from <= time.monotonic() - t0 < out_to

    def apply(t, ms, now):
        held = now - held_at.get(t, 0) < 2e9              # broker down or at its limit in the last 2 s
        for m in ms:
            e = load(m.payload())
            k = (t, e["v"])
            last = last_seq.get(k, 0)
            if e["seq"] <= last:
                st["repeats"] += 1                        # a repeat: stream it once
                continue
            if e["seq"] > last + 1:
                total["lost"] += e["seq"] - last - 1      # a gap in this vehicle's sequence: datapoints missing
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
                if is_down(t):                            # the customer's broker is down: leave the datapoints on the topic
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
                    if len(ms) == allow < a.batch:        # the destination's limit, not the topic, ended this read:
                        held_at[t], why[t] = time.time_ns(), "limit"   # the rest waits on its own topic
                        drain.setdefault(t, time.monotonic())
                    apply(t, ms, time.time_ns())
                    more = more or len(ms) == a.batch     # a full batch inside the limit: read on at once
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def backlog(t):
        tp = await admin.get_topic(STREAM, t)
        # every message in the topic minus every one this subscription has streamed (it reads from the first offset)
        return max(0, tp.messages_count - sum(nxt.get((t, pid), 0) for pid in range(mine[t]))) if tp else 0

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of datapoints arrived inside the budget; partitions interleave,
                # so one fresh datapoint alone doesn't mean the replay is finished
                if st["replayed"] and st["max_age"] < budget_ns:
                    caught_up[0] = True
                print(f"[{name}] catching up: replayed {st['replayed']:,} earlier datapoints this second", flush=True)
                st.clear()
                lat.flush()
                continue
            r = lat.flush()
            troubled = sum(1 for t in mine if time.time_ns() - held_at.get(t, 0) < 2e9)
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{name}] {r['n']:>6,} datapoints/s streamed by {len(healthy_read)} subscriptions  datapoint-to-"
                  f"connector p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} "
                  f"(<{a.budget_ms:g}ms)  behind {len(late)}/{len(mine) - troubled} healthy subscriptions  "
                  f"lost {total['lost']:,}  repeats dropped {st['repeats']:,}", flush=True)
            for t in sorted(drain):
                b = await backlog(t)
                if why.get(t) == "down" and is_down(t):
                    gone = time.monotonic() - drain[t]
                    oldest[t] = max(oldest[t], gone)
                    print(f"[{name}] broker down: {t} streamed 0/s  backlog {b:,} on its own topic, offset held at "
                          f"the last streamed datapoint  down for {gone:4.1f} s of the {window:g} s recovery window",
                          flush=True)
                elif b > a.batch or (time.time_ns() - held_at.get(t, 0) < 2e9 and st["held_" + t]):
                    oldest[t] = max(oldest[t], held_wait[t])
                    print(f"[{name}] backfilling at destination limit: {t} streamed {st['held_' + t]:,}/s (limit "
                          f"{a.rate_limit:,.0f}/s)  backlog {b:,} on its own topic  oldest waiting {held_wait[t]:5.1f} s",
                          flush=True)
                else:
                    inside = "inside" if oldest[t] < window else "OUTSIDE"
                    print(f"[{name}] {t} caught up: backlog cleared {time.monotonic() - drain[t]:.1f} s after its "
                          f"trouble began, oldest wait {oldest[t]:.1f} s {inside} the recovery window, "
                          f"{total['lost_' + t]:,} datapoints lost", flush=True)
                    del drain[t]
                    oldest[t] = 0.0
                held_wait[t] = 0.0
            st.clear()
            late.clear()
            healthy_read.clear()

    groups = [plist[r::a.readers] for r in range(a.readers)]
    print(f"[{name}] reading {len(mine)} stream subscriptions, {len(plist)} partitions, on {a.readers} connections",
          flush=True)
    await asyncio.gather(report(), *(reader(g) for g in groups if g))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--shard", type=int, default=0)
    p.add_argument("--shards", type=int, default=1, help="connector processes splitting the subscriptions")
    p.add_argument("--orgs", type=int, default=200, help="same as datapoints.py; 0 = every subscription topic in the stream")
    p.add_argument("--readers", type=int, default=2, help="connections reading this shard's partitions")
    p.add_argument("--rate-limit", type=float, default=5_000, help="datapoints a second each destination broker accepts")
    p.add_argument("--outage", default="org-0002-gps", help="subscription whose customer broker goes down")
    p.add_argument("--outage-at", type=float, default=60, help="seconds after start the broker goes down")
    p.add_argument("--outage-for", type=float, default=0, help="outage length in seconds (0 = none)")
    p.add_argument("--budget-ms", type=float, default=100, help="p99 budget, datapoint to connector, healthy subscriptions")
    p.add_argument("--poll-ms", type=float, default=5, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
