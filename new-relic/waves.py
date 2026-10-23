"""The source of truth: time-wave emitters in every cell, each cell's pipeline carrying the synthetic points to
its end, and the end-of-pipeline observer tallying what arrived, as the time-wave lane sees them.

Every --wave-s, a new generation is emitted into every cell and source pipeline: one synthetic point per Kafka
partition of that pipeline. The emitter writes {cell/pipeline, generation, points} to the lane. Each point
travels the cell's pipeline with its own delay, and every 20 ms the end-of-pipeline observer writes a tally per
cell/pipeline/generation naming the Kafka partitions whose points arrived in that tick. Kafka and the pipeline
services are not in this kit; the delays stand in for them.

Two things happen on purpose, so the watermark has something to get right:
  * a cell's Kafka goes bad: from --stall-at, --stall-share of the Kafka partitions in --stall-cells cells stop
    delivering for --stall-for seconds (an unresponsive broker), while the other partitions keep flowing. The
    held points arrive in the --drain-s after it recovers.
  * tallies repeat: --dup-rate of them are written twice, as an at-least-once bridge or a retry would.

waves.py also stands in for an alert evaluator in each cell pipeline, with one aggregation window per
generation, and compares two ways of closing a window. Event flow alone closes window g when the first point
of a window --ef-delay-s later arrives, as the default event-flow aggregation does; the truth says whether all
of window g had arrived by then. Gated by the watermark, window g closes when the watermark message for g is
received from cell-watermarks. The check also judges every behind flag: was that cell pipeline really stalled,
and how long after the stall began was it flagged?
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, LANE, PARTITIONS, PIPELINES, STREAM, WATERMARKS, Stalls, Window, cell,
                    ensure_topics, load, msg, part, wave_key)

TICK = 0.02   # seconds; the end-of-pipeline observer tallies what arrived once per tick


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LANE, WATERMARKS)
    cfg = DirectProducerConfig(batch_length=5000, linger_time=timedelta(microseconds=200))
    out = await (await own_client()).producer(STREAM, LANE, topic_partitions_count=PARTITIONS[LANE], mode=cfg)
    marks = await (await own_client()).consumer_group(
        "waves-truth-check", STREAM, WATERMARKS, polling_strategy=PollingStrategy.Next(), batch_length=5000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)),     # offsets stored once a second, not per poll
        poll_interval=timedelta(milliseconds=5))                    # a check, not on the lane's clock

    r = random.Random(11)
    kp_n = a.kafka_partitions
    keys = [wave_key(cell(c), p) for c in range(a.cells) for p in PIPELINES[:a.pipelines]]
    base = {k: r.uniform(0.05, 0.25) for k in keys}       # each cell pipeline's usual transit time, seconds
    ef_gens = max(1, round(a.ef_delay_s / a.wave_s))       # event flow's delay, in windows
    stall_cells = {cell(c) for c in r.sample(range(a.cells), a.stall_cells)} if a.stall_for else set()
    stall_parts = set(r.sample(range(kp_n), max(1, round(kp_n * a.stall_share))))
    stall_keys = {k for k in keys if k.split("/")[0] in stall_cells}
    stall = [0.0, 0.0]                                     # wall-clock seconds: [start, end] of the stall
    in_flight = []                                         # heap of (arrival time, key, generation, partition)
    left = defaultdict(dict)                               # key -> generation -> points not yet arrived
    done = defaultdict(dict)                               # key -> generation -> wall time its last point arrived
    first_seen = defaultdict(set)                          # key -> generations whose first point has arrived
    seen_wm = {}                                           # key -> watermark already judged
    flagged = {}                                           # stalled key -> seconds after the stall began
    stats, wait, stalls = defaultdict(int), Window(), Stalls()
    repeat_next = []
    gc.freeze()

    async def send_by_partition(items):
        # One batch per partition, all sent at once: per-key order kept, about one round trip per tick.
        batches = defaultdict(list)
        for key, m in items:
            batches[part(key, PARTITIONS[LANE])].append(m)
        await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))

    def emit(g, now):
        items = []
        for k in keys:
            left[k][g] = kp_n
            for p in range(kp_n):
                t = now + min(1.2, base[k] + r.expovariate(1 / 0.08))
                if k in stall_keys and p in stall_parts and stall[0] <= t < stall[1]:
                    t = stall[1] + r.uniform(0, a.drain_s)   # held by the bad broker, delivered after recovery
                heapq.heappush(in_flight, (t, k, g, p))
            items.append((k, SendMessage(msg(k=k, g=g, x=kp_n))))
        stats["emitted"] += len(keys)
        for k in keys:                                       # forget generations long gone
            left[k].pop(g - 600, None)
            done[k].pop(g - 600, None)
            first_seen[k].discard(g - 600)
        return items

    def arrivals(now):
        tallies = defaultdict(list)
        while in_flight and in_flight[0][0] <= now:
            _, k, g, p = heapq.heappop(in_flight)
            tallies[(k, g)].append(p)
            if g not in first_seen[k]:
                first_seen[k].add(g)
                close = g - 1 - ef_gens                     # event flow closes this window now
                if close in left[k]:
                    stats["ef_closed"] += 1
                    missing = left[k][close] - len(tallies.get((k, close), ()))
                    if missing > 0:
                        stats["ef_partial"] += 1    # evaluated on part of its data: the rest is still in Kafka
                        stats["ef_missing"] += missing
        items = []
        for (k, g), ps in tallies.items():
            left[k][g] = left[k].get(g, 0) - len(ps)
            if left[k][g] == 0:
                done[k][g] = now
            items.append((k, SendMessage(msg(k=k, g=g, p=ps))))
            if r.random() < a.dup_rate:
                repeat_next.append((k, g, ps))     # the same tally again next tick, as a retry would send it
        stats["tallies"] += len(items)
        return items

    async def run():
        async with out:
            t0 = time.time()
            if a.stall_for:
                stall[0], stall[1] = t0 + a.stall_at, t0 + a.stall_at + a.stall_for
            next_gen = (int(t0 / a.wave_s) + 1) * a.wave_s
            due = time.monotonic()
            while True:
                now = time.time()
                # Tallies written a second time, stamped when re-sent: a retry is a new send on the lane.
                items = [(k, SendMessage(msg(k=k, g=g, p=ps, rep=1))) for k, g, ps in repeat_next]
                stats["repeats"] += len(repeat_next)
                repeat_next.clear()
                if now >= next_gen:
                    items += emit(round(next_gen / a.wave_s), now)
                    next_gen += a.wave_s
                items += arrivals(now)
                if items:
                    await send_by_partition(items)
                due += TICK
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def on_mark(m):
        d = load(m.payload())
        k, now = d["k"], time.time()
        if "behind" in d:
            if not d["behind"]:
                stats["cleared"] += 1
                return
            if k in stall_keys and stall[0] <= now <= stall[1] + a.drain_s + a.overdue_s + 2:
                if k not in flagged:
                    flagged[k] = now - stall[0]
                    stats["behind_right"] += 1
                    if len(flagged) <= 2:
                        print(f"[waves] {k} flagged behind {now - stall[0]:.2f}s after its Kafka partitions "
                              f"stalled: generation {d['stuck']} waited {d['waited_ms'] / 1000:.2f}s, points "
                              f"missing from Kafka partitions {d['missing']}", flush=True)
            else:
                stats["behind_wrong"] += 1
                print(f"[waves] wrong behind flag: {k} at {now - stall[0]:+.2f}s from the stall, "
                      f"missing {d['missing']}", flush=True)
            return
        w = d["wm"]
        lo = seen_wm.get(k, w - 1)
        seen_wm[k] = max(lo, w)
        complete_at = 0.0
        for g in range(lo + 1, w + 1):
            if g not in left[k]:
                continue                    # before this check's memory: not judged
            stats["gated_closed"] += 1
            if left[k][g] > 0:
                stats["gated_early"] += 1   # the watermark said complete while points were still in flight
                print(f"[waves] early watermark: {k} generation {g} with {left[k][g]} points missing", flush=True)
            else:
                complete_at = max(complete_at, done[k][g])
        if complete_at:
            wait.add((now - complete_at) * 1000)

    async def report_line():
        while True:
            await asyncio.sleep(1)
            now = time.time()
            if stall[1] and now > stall[1] + a.drain_s + a.overdue_s + 3 and not stats.get("judged"):
                stats["judged"] = 1
                missed = sorted(stall_keys - set(flagged))
                stats["behind_missed_total"] = len(missed)
                if missed:
                    print(f"[waves] stalled but never flagged behind: {missed}", flush=True)
            w = wait.flush()
            stalled_now = len(stall_keys) if stall[0] <= now < stall[1] else 0
            det = max(flagged.values()) if flagged else 0.0
            print(f"[waves] {stats['emitted'] + stats['tallies'] + stats['repeats']:>6,} msgs/s  cell pipelines "
                  f"{len(keys):,} (stalled {stalled_now})  points in flight {len(in_flight):,}  tallies written twice "
                  f"{stats['repeats']:,}  | event flow alone: windows closed {stats['ef_closed']:,}, on partial data "
                  f"{stats['ef_partial']:,} (points missing {stats['ef_missing']:,})  | gated by watermark: closed "
                  f"{stats['gated_closed']:,}, early {stats['gated_early']:,}, after data complete p50 {w['p50']:.0f}ms "
                  f"max {w['max']:.0f}ms  | behind flags right {stats['behind_right']:,} (detected max "
                  f"{det:.2f}s after the stall)  wrong {stats['behind_wrong']:,}  cleared {stats['cleared']:,}  "
                  f"missed {stats.get('behind_missed_total', 0)}  {stalls.read()}", flush=True)
            for k in ("emitted", "tallies", "repeats", "ef_closed", "ef_partial", "ef_missing", "gated_closed",
                      "gated_early", "behind_right", "behind_wrong", "cleared"):
                stats[k] = 0

    await asyncio.gather(run(), marks.consume_messages(on_mark, asyncio.Event()), report_line(), stalls.watch())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--cells", type=int, default=40, help="cells, each with its own Kafka and pipeline")
    ap.add_argument("--pipelines", type=int, default=4, help="source pipelines per cell (metrics, events, logs, spans)")
    ap.add_argument("--kafka-partitions", type=int, default=64, help="Kafka partitions per pipeline: points per wave")
    ap.add_argument("--wave-s", type=float, default=1.0, help="seconds between generations (one window each here)")
    ap.add_argument("--ef-delay-s", type=float, default=2.0, help="event flow's delay before it closes a window")
    ap.add_argument("--overdue-s", type=float, default=30, help="the watermark service's --overdue-s, for judging")
    ap.add_argument("--dup-rate", type=float, default=0.02, help="share of tallies written twice")
    ap.add_argument("--stall-cells", type=int, default=1, help="cells whose Kafka goes bad")
    ap.add_argument("--stall-share", type=float, default=0.25, help="share of their Kafka partitions that stop")
    ap.add_argument("--stall-at", type=float, default=90, help="seconds after start the stall begins")
    ap.add_argument("--stall-for", type=float, default=120, help="seconds the stall lasts (0 = never)")
    ap.add_argument("--drain-s", type=float, default=2.0, help="seconds the held points take to arrive after it")
    asyncio.run(main(ap.parse_args()))
