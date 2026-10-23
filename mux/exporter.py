"""Streaming export workers, each reading its own exports at their destination's pace.

One shard of the export fleet: it owns the export topics whose crc32(name) % --shards == --shard and
reads each of their partitions directly (its own offsets, no consumer group, so no rebalances and no
tasks to restart), in turn, on --readers connections. Each export has a token bucket for its
destination's write limit: a Kinesis stream accepts 1,000 records a second per shard (one shard per
partition here), a Pub/Sub topic --pubsub-limit. A share of destination writes fails once and is
retried in place (--flaky), the "spurious write-failures that immediately succeed upon retry" Mux
describes for Kinesis; the worker keeps its offset and writes again, nothing restarts. One customer's
destination can become unreachable (--outage, --outage-at, --outage-for), as when an IAM role or a
Pub/Sub permission is changed: that export stops reading, its offset stays at the last exported
view, and its views wait on its own topic; when the destination is back it backfills from that
offset at its limit until it has caught up. Every other export on the shard reads as if nothing
happened.

For each view it drops repeats (sequence per export partition; the lane is read at least once),
counts any gap in the sequence as a lost view, counts updated views (a newer version of a view
already exported, which the customer keeps by view_id), and times it. Prints a line a second:
  * view-to-exporter latency for healthy exports, PASS or FAIL against --budget-ms;
  * how many healthy exports had any view over budget this second;
  * the share of views handed over inside Mux's one-minute promise, views lost, repeats dropped,
    updates and writes retried;
  * any export whose destination is down or at its limit: backlog on its own topic, oldest wait;
  * when that export has caught up: how long it took, views past the minute and views lost.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, KINESIS_SHARD_LIMIT, PROMISE_S, STREAM, Window, connect, exports, load, shard_of

BANK = 0.5                                                # seconds of rate limit a partition may bank for a burst


async def main(a):
    name = f"exp-{a.shard}"
    admin = await connect(a.url)
    if a.envs:                                            # the kit's own exports, from views.py
        mine = {t: parts for t, (_, _, parts) in exports(a.envs).items() if shard_of(t, a.shards) == a.shard}
        while not all([await admin.get_topic(STREAM, t) for t in mine]):
            print(f"[{name}] waiting for views.py to create the export topics…", flush=True)
            await asyncio.sleep(1)
    else:                                                 # every export topic in the stream, e.g. from kafka_bridge.py
        while not await admin.get_stream(STREAM):
            await asyncio.sleep(1)
        mine = {t.name: t.partitions_count for t in await admin.get_topics(STREAM)
                if shard_of(t.name, a.shards) == a.shard}
    plist = [(t, pid) for t, parts in mine.items() for pid in range(parts)]

    def limit(t):                                         # destination write limit, records a second
        return KINESIS_SHARD_LIMIT * mine[t] if t.endswith("kinesis") else a.pubsub_limit

    per_part = {(t, pid): limit(t) / mine[t] for t, pid in plist}
    tokens = {k: per_part[k] * BANK for k in plist}
    refilled = {k: time.monotonic() for k in plist}
    nxt = {}                                              # next offset per partition: after the last exported view
    held_at, why = {}, {}                                 # export -> when last held (ns), and why
    last_seq = {}
    budget_ns = a.budget_ms * 1_000_000
    promise_ns = PROMISE_S * 1_000_000_000
    lat, held_wait, oldest = Window(), defaultdict(float), defaultdict(float)
    st, total = defaultdict(int), defaultdict(int)
    late, healthy_read = set(), set()
    caught_up = [False]                                   # a new reader first replays what was written before it
    drain = {}                                            # export -> when its trouble began, for the catch-up report
    t0 = time.monotonic()
    out_from, out_to = a.outage_at, a.outage_at + a.outage_for
    outage = a.outage if a.outage in mine and a.outage_for > 0 else None
    gc.freeze()                                           # keep startup tables out of the collector's way

    def is_down(t):
        return t == outage and out_from <= time.monotonic() - t0 < out_to

    def apply(k, ms, now):
        t = k[0]
        held = now - held_at.get(t, 0) < 2e9              # destination down or at its limit in the last 2 s
        if random.random() < a.flaky:
            st["retried"] += 1                            # this write failed once and succeeded on retry, in place
        for m in ms:
            e = load(m.payload())
            last = last_seq.get(k, 0)
            if e["seq"] <= last:
                st["repeats"] += 1                        # a repeat: export it once
                continue
            if e["seq"] > last + 1:
                total["lost"] += e["seq"] - last - 1      # a gap in this partition's sequence: views missing
                total["lost_" + t] += e["seq"] - last - 1
            last_seq[k] = e["seq"]
            if e["ver"] > 1:
                st["updates"] += 1                        # a newer version of an exported view; the customer keys by view_id
            age = now - e["ns"]
            if age > promise_ns:
                total["past"] += 1                        # handed over more than a minute after the view finished
                total["past_" + t] += 1
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
            total["exported"] += 1

    async def reader(parts):
        c = await connect(a.url)
        consumers = {k: Consumer.Single(f"{name}-{k[0]}-{k[1]}") for k in parts}
        while True:
            more = False
            for k in parts:
                t = k[0]
                if is_down(t):                            # the customer's destination is down: views stay on the topic
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
                    apply(k, ms, time.time_ns())
                    more = more or len(ms) == a.batch     # a full batch inside the limit: read on at once
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def backlog(t):
        tp = await admin.get_topic(STREAM, t)
        # every message in the topic minus every one this export has taken (it reads from the first offset)
        return max(0, tp.messages_count - sum(nxt.get((t, pid), 0) for pid in range(mine[t]))) if tp else 0

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of views arrived inside the budget; partitions interleave,
                # so one fresh view alone doesn't mean the replay is finished
                if st["replayed"] and st["max_age"] < budget_ns:
                    caught_up[0] = True
                print(f"[{name}] catching up: replayed {st['replayed']:,} earlier views this second", flush=True)
                st.clear()
                lat.flush()
                continue
            r = lat.flush()
            troubled = sum(1 for t in mine if time.time_ns() - held_at.get(t, 0) < 2e9)
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            inside = 100.0 * (1 - total["past"] / total["exported"]) if total["exported"] else 100.0
            print(f"[{name}] {r['n']:>6,} views/s exported by {len(healthy_read)} exports  view-to-exporter "
                  f"p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"behind {len(late)}/{len(mine) - troubled} healthy exports  inside 1-min promise {inside:.2f}%  "
                  f"lost {total['lost']:,}  repeats dropped {st['repeats']:,}  updates {st['updates']:,}  "
                  f"writes retried {st['retried']:,}", flush=True)
            for t in sorted(drain):
                b = await backlog(t)
                if why.get(t) == "down" and is_down(t):
                    gone = time.monotonic() - drain[t]
                    oldest[t] = max(oldest[t], gone)
                    print(f"[{name}] destination down: {t} exported 0/s  backlog {b:,} on its own topic, offset held "
                          f"at the last exported view  down for {gone:4.1f} s", flush=True)
                elif b > a.batch or (b > 0 and time.time_ns() - held_at.get(t, 0) < 2e9 and st["held_" + t]):
                    oldest[t] = max(oldest[t], held_wait[t])
                    print(f"[{name}] backfilling at destination limit: {t} exported {st['held_' + t]:,}/s (limit "
                          f"{limit(t):,.0f}/s)  backlog {b:,} on its own topic  oldest waiting {held_wait[t]:5.1f} s",
                          flush=True)
                else:
                    print(f"[{name}] {t} caught up: backlog cleared {time.monotonic() - drain[t]:.1f} s after its "
                          f"trouble began, oldest wait {oldest[t]:.1f} s, {total['past_' + t]:,} views past the "
                          f"one-minute promise, {total['lost_' + t]:,} views lost", flush=True)
                    del drain[t]
                    oldest[t] = 0.0
                held_wait[t] = 0.0
            st.clear()
            late.clear()
            healthy_read.clear()

    groups = [plist[r::a.readers] for r in range(a.readers)]
    print(f"[{name}] reading {len(mine)} streaming exports, {len(plist)} partitions, on {a.readers} connections",
          flush=True)
    await asyncio.gather(report(), *(reader(g) for g in groups if g))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--shard", type=int, default=0)
    p.add_argument("--shards", type=int, default=1, help="export worker processes splitting the exports")
    p.add_argument("--envs", type=int, default=150, help="same as views.py; 0 = every export topic in the stream")
    p.add_argument("--readers", type=int, default=2, help="connections reading this shard's partitions")
    p.add_argument("--pubsub-limit", type=float, default=5_000, help="records a second a customer's Pub/Sub topic takes")
    p.add_argument("--flaky", type=float, default=0.01, help="share of destination writes that fail once, then succeed")
    p.add_argument("--outage", default="env-0001-kinesis", help="export whose customer destination becomes unreachable")
    p.add_argument("--outage-at", type=float, default=60, help="seconds after start the destination goes away")
    p.add_argument("--outage-for", type=float, default=0, help="outage length in seconds (0 = none)")
    p.add_argument("--budget-ms", type=float, default=100, help="p99 budget, view to export worker, healthy exports")
    p.add_argument("--poll-ms", type=float, default=5, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
