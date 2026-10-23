"""A surge worker for one region, fed by the lane: demand and supply events in, one closed window per --window-s.

Run one per region. Each reads the whole surge-events topic directly, partition by partition, on --readers
connections of its own (no consumer group: a group reader visits one partition per poll, so with 16 partitions
an event waited for its partition's turn). Nothing is shared between regions except the log: each region
computes every window independently from the same input, so their results converge, the way Uber describes
surge running a redundant pipeline in every region.

For each window, per hexagon and product: distinct riders (demand) and distinct available drivers (supply),
from the event time, so an event counts in the window it happened in. Counting distinct IDs means a second
delivery of the same event cannot change a count. A window closes --grace-s after it ends; an event that
arrives after that does not count and is reported as late, as late messages do not contribute to surge.

At close, the worker computes a multiplier per cell (common.multipliers, a placeholder for the surge model),
publishes a fingerprint of the window to window-digests, and, if it is the --primary region, every cell's
multiplier to surge-multipliers. It checks each window against the marketplace's own count (window-truth) and
against the other region's fingerprint.

Prints event-to-window p50/p99/p99.9 every second with PASS/FAIL against --budget-ms, and the window numbers:
the last window closed, how long after its end its last event arrived, cells surging, windows exact against the
independent count, late events, windows rebuilt from the log after a restart, and windows where both regions
agree.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, DIGESTS, EVENTS, MULTIPLIERS, PARTITIONS, STREAM, TRUTH, Window, digest,
                    ensure_topics, load, msg, multipliers, part, window)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), EVENTS, MULTIPLIERS, DIGESTS, TRUTH)
    dg_out = await (await own_client()).producer(STREAM, DIGESTS, topic_partitions_count=PARTITIONS[DIGESTS])
    m_out = await (await own_client()).producer(STREAM, MULTIPLIERS, topic_partitions_count=PARTITIONS[MULTIPLIERS])
    ws_ms = int(a.window_s * 1000)
    lat, stats = Window(), defaultdict(float)
    open_w = defaultdict(lambda: defaultdict(lambda: (set(), set())))   # window -> cell -> riders, drivers
    after = {}                              # window -> latest arrival after its end, ms (live events only)
    rebuilt = set()                         # windows that took events while this worker was replaying
    closed_ids = set()                      # every window this worker has closed
    results = {}                            # window -> {cell: [riders, drivers]}, kept for the check
    truth = defaultdict(dict)               # window -> {part: counts} from the marketplace
    tot = defaultdict(float)                # since start: exact, mismatched, late, rebuilt, agree, disagree
    regions = defaultdict(dict)             # window -> {region: fingerprint}
    compared = set()
    last_close = {}
    caught_up = [False]                     # a new worker first replays the log from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_event(m):
        e = load(m.payload())
        now_ns = time.time_ns()
        age = (now_ns - e["ns"]) / 1e6
        w = window(e["t"], a.window_s)
        if not caught_up[0]:                # still replaying: track the oldest event seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
            if age > a.budget_ms:
                rebuilt.add(w)              # an event from before this worker joined: the window is rebuilt
        else:
            lat.add(age)                    # live events, from the marketplace's send to applied in the window
            late_by = now_ns // 1_000_000 - (w + 1) * ws_ms
            if late_by > after.get(w, -10 ** 9):
                after[w] = late_by
        stats["applied"] += 1
        if w in closed_ids:
            tot["late"] += 1                # its window already closed: it no longer counts toward surge
            return
        cell = open_w[w][f"{e['h']}|{e['p']}"]
        (cell[0] if e["k"] == "d" else cell[1]).add(e["u"])

    def check(w):
        t = truth.get(w)
        if w not in results or not t or len(t) != next(iter(t.values()))["n"]:
            return                          # not closed here yet, or not every part of the count has arrived
        exp = {}
        for p in t.values():
            exp.update(p["g"])
        got = {k: v for k, v in results.pop(w).items() if v[0] or v[1]}
        tot["exact" if got == {k: v for k, v in exp.items() if v[0] or v[1]} else "mismatched"] += 1
        del truth[w]

    async def close_windows():
        async with dg_out, m_out:
            while True:
                await asyncio.sleep(0.05)
                if not caught_up[0]:
                    continue
                now_ms = time.time_ns() // 1_000_000
                for w in sorted(x for x in list(open_w) if (x + 1) * ws_ms + a.grace_s * 1000 <= now_ms):
                    cells = open_w.pop(w)
                    rows = {k: [len(r), len(s)] for k, (r, s) in cells.items()}
                    mult = multipliers(rows)
                    closed_ids.add(w)
                    results[w] = rows
                    await dg_out.send([SendMessage(msg(region=a.region, w=w, dg=digest(rows, mult), n=len(rows)))])
                    if a.primary:
                        out = defaultdict(list)
                        for k, (d, s) in rows.items():
                            h, p = k.split("|")
                            out[part(h, PARTITIONS[MULTIPLIERS])].append(
                                SendMessage(msg(w=w, h=h, p=p, m=mult[k], d=d, s=s)))
                        for q, ms in out.items():
                            for i in range(0, len(ms), 1000):
                                await m_out.send_with_partitioning(ms[i:i + 1000], Partitioning.partition_id(q))
                        stats["multipliers"] += len(rows)
                    was_rebuilt = w in rebuilt
                    tot["rebuilt"] += was_rebuilt
                    surging = [m for m in mult.values() if m > 1.0]
                    last_close.update(w=w, cells=len(rows), riders=sum(v[0] for v in rows.values()),
                                      drivers=sum(v[1] for v in rows.values()), surging=len(surging),
                                      max=max(surging, default=1.0), closed=now_ms - (w + 1) * ws_ms,
                                      after=None if was_rebuilt else after.pop(w, None))
                    check(w)
                for w in list(truth):
                    check(w)
                old = window(now_ms, a.window_s) - 10            # keep ten windows for the check
                for d in (results, truth):
                    for w in [w for w in d if w < old]:
                        del d[w]

    async def read(pids):
        # Each partition in turn, from the start of the log (a restarted worker rebuilds its open windows from
        # it), then from our own position; nothing is stored on the broker. A full batch means more is waiting.
        c = await own_client()
        who = Consumer.Single(f"{a.region}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, EVENTS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_event(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def read_small(topic, apply):
        c = await own_client()
        who = Consumer.Single(f"{a.region}-{topic}-{os.getpid()}")
        nxt = None
        while True:
            ms = await c.poll_messages(STREAM, topic, consumer=who, partition_id=0, count=100, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    apply(load(m.payload()))
            await asyncio.sleep(0.1)

    def on_truth(t):
        truth[t["w"]][t["i"]] = t

    def on_digest(d):
        regions[d["w"]].setdefault(d["region"], d["dg"])     # the first fingerprint each region published
        r = regions[d["w"]]
        if len(r) >= 2 and d["w"] not in compared:
            compared.add(d["w"])
            tot["agree" if len(set(r.values())) == 1 else "disagree"] += 1

    async def report():
        role = "primary" if a.primary else "standby"
        print(f"[{a.region}] surge worker ({role}): window {a.window_s:g} s, grace {a.grace_s:g} s, "
              f"budget {a.budget_ms} ms", flush=True)
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.region}] catching up: replayed {int(stats['replayed']):,} earlier events this second",
                      flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.region}] {r['n']:>6,} events/s  event-to-window p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"multipliers sent {int(stats['multipliers']):,}", flush=True)
            if last_close:
                lc = last_close
                tail = "rebuilt from the log" if lc["after"] is None else f"last event {lc['after']:+,} ms after its end"
                print(f"[window] {a.region} closed {lc['w'] % 100000} +{lc['closed']:,} ms after its end: "
                      f"{lc['cells']:,} cells, riders {lc['riders']:,}, drivers {lc['drivers']:,}, "
                      f"surging {lc['surging']} (max {lc['max']:.1f}x)  {tail}  "
                      f"exact {int(tot['exact'])}/{int(tot['exact'] + tot['mismatched'])}  "
                      f"late {int(tot['late'])}  rebuilt {int(tot['rebuilt'])}  "
                      f"regions agree {int(tot['agree'])}/{int(tot['agree'] + tot['disagree'])}", flush=True)
            stats.clear()

    parts = list(range((await (await own_client()).get_topic(STREAM, EVENTS)).partitions_count))
    await asyncio.gather(close_windows(), read_small(TRUTH, on_truth), read_small(DIGESTS, on_digest), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--region", default="region-a", help="this worker's region name")
    p.add_argument("--primary", action="store_true", help="publish multipliers (one region does)")
    p.add_argument("--readers", type=int, default=2, help="connections reading the events partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--budget-ms", type=int, default=50, help="event-to-window p99 budget for the lane")
    p.add_argument("--window-s", type=float, default=60, help="the surge window (match marketplace.py)")
    p.add_argument("--grace-s", type=float, default=1, help="close a window this long after it ends")
    asyncio.run(main(p.parse_args()))
