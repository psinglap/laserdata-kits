"""The streaming aggregator, fed by the lane: raw samples in, one aggregated series per group per timeslice out.

Reads the samples topic directly, partition by partition, on --readers connections of its own (no consumer
group: a group reader visits one partition per poll, so with 16 partitions a sample waited for its partition's
turn). Every sample for one shard key (cluster, metric, service) is on one partition, so this aggregator holds
all the data its rule needs, and runs it:

    sum2 by (cluster, service, le) (http_requests_duration_seconds_bucket[timeslice])

Each sample carries the counter's value (the cumulative count) and a per-series sequence, so the aggregator
adds only what is new:
  * a sample whose sequence it has already applied (a retry, or a replay after a restart) adds nothing;
  * a newer one adds its value minus the last value applied for that series; a value lower than the last one is
    a counter reset (the process restarted), and adds the value itself, as sum2 does;
  * a series' first sample is its baseline and adds nothing.
Apache Iggy delivers at least once; counting once comes from this rule. Without the sequence check, a retried
old sample is lower than the value before it and looks exactly like a counter reset, so it is added whole.

A timeslice closes --grace-s after it ends. Then its groups are published to aggregates as resulting values,
and checked against the gateway's own count for that timeslice (window-truth): exact, or mismatched.
A sample that arrives after its timeslice closed is counted as late.

Prints sample-to-aggregator p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the aggregation
numbers: timeslices closed and checked exact, series in and out, retries absorbed, counter resets, late samples,
and what an aggregator without the sequence check would have over-counted.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (AGGREGATES, DEFAULT_URL, OUTPUT, PARTITIONS, RULE, SAMPLES, STREAM, TRUTH, Window,
                    ensure_topics, load, msg, window)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SAMPLES, AGGREGATES, TRUTH)
    out = await (await own_client()).producer(STREAM, AGGREGATES, topic_partitions_count=PARTITIONS[AGGREGATES])
    ts_ms = int(a.timeslice_s * 1000)
    lat, stats = Window(), defaultdict(float)
    last = {}                               # series -> [sequence, value, timeslice] last applied
    naive = {}                              # series -> value, for an aggregator without the sequence check
    open_w = defaultdict(lambda: defaultdict(int))   # timeslice -> group -> increase so far
    series_in = defaultdict(set)            # timeslice -> series seen in it
    closed = {}                             # timeslice -> {group: increase}, kept for the check
    truth = {}                              # timeslice -> the gateway's own count, waiting for its close
    tot = defaultdict(float)                # since start: exact, mismatched, absorbed, resets, late, sum, naive
    last_close = {}                         # the most recent close, for the report
    caught_up = [False]                     # a new reader first replays samples from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_sample(m):
        s = load(m.payload())
        age = (time.time_ns() - s["ns"]) / 1e6
        key = (s["c"], s["sv"], s["id"], s["le"])
        v = s["v"]
        # what an aggregator that trusts every copy would add
        nv = naive.get(key)
        if nv is not None:
            tot["naive"] += v - nv if v >= nv else v
        naive[key] = v
        prev = last.get(key)
        if prev is not None and s["seq"] <= prev[0]:
            tot["absorbed"] += 1            # a retry or a replay: already counted
            return
        if not caught_up[0]:                # still replaying: track the oldest new sample seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        w = window(s["t"], a.timeslice_s)
        if prev is None:
            add = 0                         # a new series: its first sample is the baseline
        elif v >= prev[1]:
            add = v - prev[1]
        else:
            add = v                         # a counter reset: counting again from zero (sum2)
            tot["resets"] += 1
        last[key] = [s["seq"], v, w]
        stats["applied"] += 1
        if caught_up[0]:
            lat.add(age)                    # live samples, from the gateway's send to applied in the aggregator
        if w in closed:
            tot["late"] += 1                # its timeslice already closed: it can no longer count
            tot["late_add"] += add          # left out of the comparison too: dropped here, not over-counted there
            return
        g = f"{s['c']}|{s['sv']}|{s['le']}"
        if add:
            open_w[w][g] += add
            tot["sum"] += add
        series_in[w].add(key)

    def check(w):
        got = {g: v for g, v in closed[w].items() if v}
        exp = {g: v for g, v in truth.pop(w).items() if v}
        tot["exact" if got == exp else "mismatched"] += 1

    async def close_windows():
        # Close each timeslice --grace-s after it ends, once the replay is done, and publish its groups.
        async with out:
            while True:
                await asyncio.sleep(0.1)
                if not caught_up[0]:
                    continue
                now_ms = time.time_ns() // 1_000_000
                for w in sorted(x for x in list(open_w) + list(series_in) if (x + 1) * ts_ms + a.grace_s * 1000 <= now_ms):
                    if w in closed:
                        continue
                    groups = open_w.pop(w, {})
                    n_in = len(series_in.pop(w, ()))
                    closed[w] = dict(groups)
                    batch = [SendMessage(msg(name=OUTPUT, w=w, c=g.split("|")[0], sv=g.split("|")[1],
                                             le=g.split("|")[2], v=v)) for g, v in groups.items()]
                    for i in range(0, len(batch), 1000):
                        await out.send(batch[i:i + 1000])
                    stats["aggregates"] += len(batch)
                    last_close.update(w=w, n_in=n_in, n_out=len(groups),
                                      after=now_ms - (w + 1) * ts_ms)
                    if w in truth:
                        check(w)
                for w in [w for w in truth if w in closed]:
                    check(w)
                old = window(now_ms, a.timeslice_s) - 5      # keep five timeslices for the check
                for d in (closed, truth):
                    for w in [w for w in d if w < old]:
                        del d[w]

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, SAMPLES, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_sample(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def read_truth():
        c = await own_client()
        who = Consumer.Single(f"{a.name}-truth-{os.getpid()}")
        nxt = None
        while True:
            ms = await c.poll_messages(STREAM, TRUTH, consumer=who, partition_id=0, count=100, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    t = load(m.payload())
                    truth[t["w"]] = t["g"]
            await asyncio.sleep(0.1)

    async def report():
        print(f"[{a.name}] rule: {RULE} as {OUTPUT}, timeslice {a.timeslice_s:g} s, grace {a.grace_s:g} s",
              flush=True)
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of samples arrived inside the budget; partitions interleave,
                # so one fresh sample alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier samples this second",
                      flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} samples/s  sample-to-aggregator p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"aggregates sent {int(stats['aggregates']):,}", flush=True)
            if last_close:
                lc = last_close
                fold = lc["n_in"] / lc["n_out"] if lc["n_out"] else 0
                # the naive aggregator saw every sample, including late ones this aggregator dropped; compare
                # like with like, so a late drop is never reported as the naive side over-counting
                over = tot["naive"] - tot["sum"] - tot["late_add"]
                print(f"[timeslice] last closed {lc['w'] % 100000} +{lc['after']:,} ms after its end: "
                      f"series in {lc['n_in']:,} -> out {lc['n_out']:,} ({fold:,.0f}x fewer)  "
                      f"exact {int(tot['exact'])}/{int(tot['exact'] + tot['mismatched'])}  "
                      f"retries absorbed {int(tot['absorbed']):,}  counter resets {int(tot['resets']):,}  "
                      f"late {int(tot['late'])}  without seq check +{over:,.0f} "
                      f"({100 * over / tot['sum'] if tot['sum'] else 0:.1f}%)", flush=True)
            stats.clear()

    parts = list(range((await (await own_client()).get_topic(STREAM, SAMPLES)).partitions_count))
    await asyncio.gather(close_windows(), read_truth(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the samples partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="aggregator")
    p.add_argument("--budget-ms", type=int, default=50, help="sample-to-aggregator p99 budget for the lane")
    p.add_argument("--timeslice-s", type=float, default=60, help="the aggregation timeslice (match gateway.py)")
    p.add_argument("--grace-s", type=float, default=2, help="wait this long after a timeslice ends to close it")
    asyncio.run(main(p.parse_args()))
