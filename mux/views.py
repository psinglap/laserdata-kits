"""The finished-view writer: every finished (or updated) view to its environment's streaming export.

Plays the end of the Mux Data processors: a processor "accumulates beacons for each view it's
tracking until the view ends", then the finished view goes on. Here each finished view is written to
the export topic of its environment (long-tail sizes; the largest media customers produce most
views), keyed by view ID, with the fields a VideoView export carries (view_id, player, country,
watch time, startup time, rebuffer percentage; padded to --size bytes like a full record). A share
of views is updated after it was exported and re-emitted with a higher version, as Mux documents
("a view can be updated after it has been exported"). Every record carries a sequence per export
partition, so an export worker can drop repeats and prove nothing is missing.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import Partitioning, SendMessage

from common import (COUNTRIES, DEFAULT_URL, PLAYERS, STREAM, connect, ensure_topic, export_name, exports, msg, part,
                    partitions_for, view_id, weights)


async def main(a):
    admin = await connect(a.url)
    n = a.envs
    ex = exports(n)
    for t, (_, _, parts) in ex.items():
        await ensure_topic(admin, STREAM, t, parts)
    print(f"[views] {len(ex)} streaming exports ready in '{STREAM}' for {n} environments", flush=True)
    writers = [await connect(a.url) for _ in range(a.writers)]
    rate = [a.views_per_s * x for x in weights(n)]        # finished views a second per environment
    routes = [(export_name(i), partitions_for(i, n), i % a.writers) for i in range(n)]
    pad = os.urandom(max(0, a.size - 260) // 2).hex()      # stands in for the rest of the VideoView fields
    seq = defaultdict(int)                                 # (topic, partition) -> last sequence sent
    recent = [deque(maxlen=500) for _ in range(n)]         # recently finished views, for later updates
    owed = [0.0] * n
    made = [0] * n
    stats = defaultdict(int)
    gc.freeze()                                            # keep the collector from pausing the sender

    async def send(c, batch):
        await asyncio.gather(*(c.send_messages(STREAM, t, Partitioning.partition_id(pid), ms)
                               for (t, pid), ms in batch.items()))

    def record(i, t, parts, v, ver, batches, wr):
        pid = part(v, parts)
        seq[t, pid] += 1
        d = dict(ns=time.time_ns(), view_id=v, ver=ver, seq=seq[t, pid], env=i + 1,
                 player=random.choice(PLAYERS), country=random.choice(COUNTRIES),
                 watch_ms=random.randint(5_000, 3_600_000), startup_ms=random.randint(300, 4_000),
                 rebuffer_pct=round(random.random() * random.random() * 5, 3), p=pad)
        batches[wr][t, pid].append(SendMessage(msg(**d)))

    async def run():
        tick = time.monotonic()
        while True:
            batches = [defaultdict(list) for _ in writers]
            for i in range(n):
                owed[i] += rate[i] * a.tick_ms / 1000
                k = int(owed[i])
                owed[i] -= k
                t, parts, wr = routes[i]
                for _ in range(k):
                    if recent[i] and random.random() < a.update_share:
                        v, ver = random.choice(recent[i])  # a view updated after export: re-emitted, newer version
                        record(i, t, parts, v, ver + 1, batches, wr)
                        stats["updated"] += 1
                        continue
                    made[i] += 1
                    v = view_id(i, made[i])
                    recent[i].append((v, 1))
                    record(i, t, parts, v, 1, batches, wr)
                    stats["finished"] += 1
            # One batch per export partition per tick, each writer on its own connection.
            await asyncio.gather(*(send(c, b) for c, b in zip(writers, batches) if b))
            tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)   # never bank more than 100 ms
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            print(f"[views] {stats['finished']:>6,} finished views/s and {stats['updated']:>4,} updated views/s "
                  f"from {n} environments to {len(ex)} streaming exports", flush=True)
            stats.clear()

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--envs", type=int, default=150, help="environments with a streaming export on this cluster")
    p.add_argument("--views-per-s", type=float, default=2500, help="finished views a second across those environments")
    p.add_argument("--update-share", type=float, default=0.03, help="share of records that update an exported view")
    p.add_argument("--size", type=int, default=1200, help="bytes per view record")
    p.add_argument("--writers", type=int, default=2, help="connections sending in parallel")
    p.add_argument("--tick-ms", type=float, default=20)
    asyncio.run(main(p.parse_args()))
