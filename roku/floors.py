"""The floor service, fed by the auction-outcome lane.

Reads every partition of auction-outcomes directly, on --readers connections of its own, from the start of the
log, tracking its own position (no consumer group: a group reader visits one partition per poll, so with 16
partitions a record waits for its partition's turn). Each record is one auction on one exchange node: the
inventory segment, how many bids came back in time, the top and second bid, and a per-(node, segment)
sequence, so a replayed or repeated record is dropped and counted once.

For every segment it keeps the top bids of the last --window-s seconds and, every --every-ms, sets the floor
to the --q quantile of them (a deliberately simple rule; your floor model goes here). A floor that moved by
more than --min-move is published on floor-updates, which every exchange node reads (exchange.py).

Prints outcome-to-floor-service p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and how many
floors moved, with the live category's average floor so you can watch it follow a live break.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import FLOORS, LIVE, OUTCOMES, PARTITIONS, STREAM, DEFAULT_URL, Window, ensure_topics, load, msg


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await own_client()
    await ensure_topics(admin, OUTCOMES, FLOORS)
    parts = list(range((await admin.get_topic(STREAM, OUTCOMES)).partitions_count))
    out = await (await own_client()).producer(STREAM, FLOORS, topic_partitions_count=PARTITIONS[FLOORS])
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (node, segment) -> last sequence applied
    tops = defaultdict(deque)               # segment -> (event second, top bid) inside the window
    dirty = set()                           # segments with new outcomes since the last recompute
    floor, fseq = {}, defaultdict(int)      # segment -> current floor (USD CPM), floor sequence
    pending = []                            # floor updates waiting for the next send
    caught_up = [False]                     # a new reader first replays outcomes from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_outcome(m, now):
        o = load(m.payload())
        age = (now - o["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = (o["x"], o["s"])
        if o["seq"] <= seq.get(k, 0):
            stats["dropped"] += 1           # replayed or repeated: counted once
            return
        seq[k] = o["seq"]
        stats["auctions"] += 1
        if o["top"] > 0:
            tops[o["s"]].append((o["ns"] / 1e9, o["top"]))
            dirty.add(o["s"])

    async def read(pids):
        # Each partition in turn, from the start of the log, then from our own position; nothing is stored on
        # the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, OUTCOMES, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_outcome(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def recompute():
        # From its due time, not from "now", so a slow round does not drift the schedule.
        due = time.monotonic()
        while True:
            due += a.every_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            cutoff = time.time() - a.window_s
            for s in list(dirty):
                d = tops[s]
                while d and d[0][0] < cutoff:
                    d.popleft()
                if len(d) < a.min_samples:
                    continue
                v = sorted(x for _, x in d)
                f = round(v[int(a.q * len(v))], 4)
                old = floor.get(s, 0.0)
                if not old or abs(f - old) / old > a.min_move:
                    floor[s] = f
                    fseq[s] += 1
                    pending.append(SendMessage(msg(s=s, seq=fseq[s], floor=f)))
                    stats["moved"] += 1
            dirty.clear()

    async def send_floors():
        # Floors come in bursts after each recompute; one batch every 2 ms, never one send per floor.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of outcomes arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier outcomes this second",
                      flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            live = [f for s, f in floor.items() if s.startswith(LIVE + "/")]
            print(f"[{a.name}] {r['n']:>7,} outcomes/s  outcome-to-floor-service p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"floors moved {int(stats['moved']):,}  sent {int(stats['sent']):,}  segments priced {len(floor):,}  "
                  f"{LIVE} avg floor ${sum(live) / len(live) if live else 0:.2f}  repeats dropped {int(stats['dropped']):,}",
                  flush=True)
            stats.clear()

    await asyncio.gather(recompute(), send_floors(), report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="floors")
    p.add_argument("--budget-ms", type=int, default=50, help="outcome-to-floor-service p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the outcome partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--window-s", type=float, default=5, help="seconds of top bids each floor is computed from")
    p.add_argument("--every-ms", type=float, default=250, help="how often floors are recomputed")
    p.add_argument("--q", type=float, default=0.05, help="floor = this quantile of the segment's recent top bids")
    p.add_argument("--min-samples", type=int, default=10, help="auctions with bids needed before a floor is set")
    p.add_argument("--min-move", type=float, default=0.02, help="publish a floor only when it moved this much")
    asyncio.run(main(p.parse_args()))
