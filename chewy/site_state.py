"""The floor view inside the building, fed by the site lane: every carton's newest step, in milliseconds.

This is what the building reads to keep moving: the WMS workflow, the associates' handhelds and the warehouse
control system ask it where a carton is and what happens next. It reads the site broker in the same building,
so it keeps working when the link to AWS is down.

Reads the carton-events topic directly, partition by partition, on --readers connections of its own (no consumer
group: a group reader visits one partition per poll, so a message waits for its partition's turn). Each event
carries the carton's per-carton sequence and resulting step:
  * an event whose sequence it has already applied (a handheld retry, or a replay after a restart) changes nothing;
  * a newer one sets the carton's step; a sequence that skips one is counted as a gap (there should be none).
Apache Iggy delivers at least once; applying once comes from this rule.

Prints scan-to-floor-view p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the floor: cartons
at each step, ship-confirmed, retries absorbed and gaps.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import Counter, defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, DONE, EVENTS, FC, STEPS, Window, connect, ensure_topics, load


async def main(a):
    await ensure_topics(await connect(a.url), EVENTS)
    lat, stats = Window(), defaultdict(float)
    last = {}                               # carton -> sequence applied
    at = Counter()                          # step -> cartons whose newest step it is
    tot = defaultdict(int)                  # since start: confirmed, absorbed, gaps
    caught_up = [False]                     # a new reader first replays events from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_event(m):
        e = load(m.payload())
        age = (time.time_ns() - e["ns"]) / 1e6
        c, seq = e["carton"], e["seq"]
        prev = last.get(c, 0)
        if seq <= prev:
            tot["absorbed"] += 1            # a retry or a replay: already applied
            return
        if seq > prev + 1:
            tot["gaps"] += 1                # an event this view never saw
        if prev:
            at[STEPS[prev - 1]] -= 1
        last[c] = seq
        if seq == DONE:
            tot["confirmed"] += 1
        else:
            at[e["step"]] += 1
        if not caught_up[0]:                # still replaying: track the oldest new event seen this second
            stats["replayed"] += 1          # (a retry carries its original send time, so it is left out)
            stats["max_age"] = max(stats["max_age"], age)
            return
        stats["applied"] += 1
        lat.add(age)                        # live: from the device's send to the floor view updated

    async def read(pids):
        # Each partition in turn, from the start of the log (the view is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await connect(a.url)
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(FC, EVENTS, consumer=who, partition_id=p, count=a.batch,
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

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier carton events this second",
                      flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>6,} carton events/s  scan-to-floor-view p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)", flush=True)
            steps = "  ".join(f"{s.lower()} {at[s]:,}" for s in STEPS[:-1])
            print(f"[floor view] cartons {len(last):,}  on the floor: {steps}  ship-confirmed {tot['confirmed']:,}  "
                  f"retries absorbed {tot['absorbed']:,}  gaps {tot['gaps']:,}", flush=True)
            stats.clear()

    parts = list(range((await (await connect(a.url)).get_topic(FC, EVENTS)).partitions_count))
    await asyncio.gather(report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL, help="the building's broker")
    p.add_argument("--readers", type=int, default=2, help="connections reading the lane's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=2000)
    p.add_argument("--name", default="site")
    p.add_argument("--budget-ms", type=int, default=50, help="scan-to-floor-view p99 budget")
    asyncio.run(main(p.parse_args()))
