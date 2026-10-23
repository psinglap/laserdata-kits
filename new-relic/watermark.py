"""The watermark service alert evaluators ask before they close a window, fed by the time-wave lane.

Reads every partition of wave-lane directly, on --readers connections of its own, from the start of the
retained log and then from its own position. For every cell and source pipeline it keeps the time-wave
generations still in flight: how many synthetic points each one was emitted with (one per Kafka partition of
that pipeline) and which partitions' points have reached the end of the pipeline. A generation is complete when
every point has arrived, and the watermark is the newest generation such that it and every one before it are
complete: data stamped up to that generation has made it through the cell.

  watermark  each time a cell/pipeline's watermark moves, it goes to cell-watermarks. An evaluator closes an
             aggregation window only when the watermark has passed the window's end, so a window is never
             evaluated on part of its data.
  behind     when the oldest incomplete generation has waited more than --overdue-s, the cell/pipeline is
             flagged behind, with the Kafka partitions whose points are missing, while the blockage is still
             there. The flag clears when the watermark moves past it.

Arrival tallies name partitions rather than counting them, so a tally delivered twice (the lane, like any
at-least-once bus, can repeat one) changes nothing. Watermark messages go out in one batch every 10 ms, from
their own task, never from the read loop. A restarted service rebuilds every watermark from the retained
lane: it holds nothing that the log does not, and stores no offsets on the broker.

Prints wave-to-watermark p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, WATERMARKS, Stalls, Window, ensure_topics, load, msg


class Gen:
    __slots__ = ("x", "got", "emit_ns")

    def __init__(self):
        self.x, self.got, self.emit_ns = None, set(), 0


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LANE, WATERMARKS)
    out = await (await own_client()).producer(STREAM, WATERMARKS, topic_partitions_count=PARTITIONS[WATERMARKS])

    gens = defaultdict(dict)                # cell/pipeline -> generation -> Gen
    wm = {}                                 # cell/pipeline -> watermark (newest complete-through generation)
    behind = {}                             # cell/pipeline -> generation it is stuck on, while flagged
    outbox = []                             # watermark and behind messages waiting for the sender task
    overdue_ns = int(a.overdue_s * 1e9)
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    caught_up = [False]                     # a new reader first replays what is retained on the lane
    gc.freeze()                             # keep start-up objects out of the collector's way

    def timed(d):
        age = (time.time_ns() - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest message seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    def advance(k):
        w, gs, moved = wm[k], gens[k], False
        while (g := gs.get(w + 1)) is not None and g.x is not None and len(g.got) >= g.x:
            del gs[w + 1]
            w += 1
            moved = True
        if moved:
            wm[k] = w
            outbox.append(SendMessage(msg(k=k, wm=w)))
            stats["wm_sent"] += 1
            if k in behind and behind[k] <= w:
                del behind[k]
                outbox.append(SendMessage(msg(k=k, behind=False, wm=w)))

    def on_message(m):
        d = load(m.payload())
        timed(d)
        k, g = d["k"], d["g"]
        if "x" in d:                        # a generation emitted into the cell's pipeline
            if k not in wm:
                wm[k] = g - 1               # first sight: count from this generation on
            if g <= wm[k]:
                stats["repeats"] += 1
                return
            gen = gens[k].setdefault(g, Gen())
            gen.x, gen.emit_ns = d["x"], d["ns"]
        else:                               # a tally: these partitions' points reached the end of the pipeline
            if k in wm and g <= wm[k]:
                stats["repeats"] += 1       # already complete: a repeated tally changes nothing
                return
            gen = gens[k].setdefault(g, Gen())
            before = len(gen.got)
            gen.got.update(d["p"])
            if len(gen.got) == before:
                stats["repeats"] += 1
                return
        if k in wm:
            advance(k)

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, LANE, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_message(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def overdue():
        # Every --check-ms, from the due time: is any cell/pipeline's oldest incomplete generation overdue?
        due = time.monotonic()
        while True:
            due += a.check_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            if not caught_up[0]:
                continue
            now = time.time_ns()
            for k, w in wm.items():
                g = gens[k].get(w + 1)
                if g is None or g.x is None or k in behind or now - g.emit_ns <= overdue_ns:
                    continue
                behind[k] = w + 1
                missing = sorted(set(range(g.x)) - g.got)
                outbox.append(SendMessage(msg(k=k, behind=True, wm=w, stuck=w + 1, missing=missing[:64],
                                              waited_ms=round((now - g.emit_ns) / 1e6))))
                stats["flagged"] += 1

    async def sender():
        async with out:
            while True:
                await asyncio.sleep(0.01)
                if outbox:
                    batch = outbox[:]
                    outbox.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report_line():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of messages arrived inside the budget; partitions interleave,
                # so one fresh message alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier messages this second, "
                      f"watermarks rebuilt for {len(wm):,} cell pipelines", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            inflight = sum(len(v) for v in gens.values())
            print(f"[{a.name}] {r['n']:>7,} msgs/s  wave-to-watermark p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  cell pipelines {len(wm):,}  behind now "
                  f"{len(behind):,}  generations in flight {inflight:,}  watermarks sent {int(stats['wm_sent']):,}  "
                  f"behind flags {int(stats['flagged']):,}  repeats ignored {int(stats['repeats']):,}  "
                  f"{stalls.read()}", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[LANE]))
    await asyncio.gather(*(read(parts[r::a.readers]) for r in range(a.readers)),
                         overdue(), sender(), report_line(), stalls.watch())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="watermark")
    p.add_argument("--budget-ms", type=int, default=50, help="wave-to-watermark p99 budget for the lane")
    p.add_argument("--overdue-s", type=float, default=30,
                   help="a generation not complete this long after it was emitted flags its cell pipeline behind")
    p.add_argument("--check-ms", type=float, default=100, help="how often to look for overdue generations")
    p.add_argument("--readers", type=int, default=2, help="connections reading the lane's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
