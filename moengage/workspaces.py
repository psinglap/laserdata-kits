"""Brands' trigger events, one Apache Iggy topic per workspace.

Plays the point where MoEngage has validated an incoming SDK or API event and knows its
workspace: every trigger event goes to that workspace's topic, keyed by user, with a
per-user sequence number. Workspace sizes follow a long tail (a few large brands, many
small ones). One brand can run a flash sale: --flash-rate extra events a second on its
workspace for --flash-for seconds, the kind of spike MoEngage's reliability post describes.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import (DEFAULT_URL, EVENTS, STREAM, connect, ensure_topic, msg, part, partitions_for, weights,
                    ws_name)


async def main(a):
    admin = await connect(a.url)
    n = a.workspaces
    parts = [partitions_for(i, n) for i in range(n)]
    for i in range(n):
        await ensure_topic(admin, STREAM, ws_name(i), parts[i])
    print(f"[workspaces] {n} workspace topics ready in '{STREAM}' "
          f"({sum(p > 1 for p in parts)} large brands with 4 partitions)", flush=True)
    writers = [await connect(a.url) for _ in range(a.writers)]
    w = weights(n)
    users = [max(200, int(a.users * x)) for x in w]
    names, shares = zip(*EVENTS)
    seq = defaultdict(int)                       # (workspace, user) -> last sequence sent
    owed = [0.0] * n
    flash = a.flash_ws - 1
    stats = defaultdict(int)
    gc.freeze()                                  # keep the collector from pausing the sender

    def event(i):
        u = f"u{random.randrange(users[i]):07d}"
        seq[i, u] += 1
        return u, SendMessage(msg(ws=i, u=u, seq=seq[i, u], e=random.choices(names, shares)[0]))

    async def send(c, batch):
        await asyncio.gather(*(c.send_messages(STREAM, ws_name(i), Partitioning.partition_id(pid), ms)
                               for (i, pid), ms in batch.items()))

    async def run():
        t0 = tick = time.monotonic()
        while True:
            t = time.monotonic() - t0
            burst = a.flash_at <= t < a.flash_at + a.flash_for and 0 <= flash < n
            batches = [defaultdict(list) for _ in writers]
            for i in range(n):
                owed[i] += (a.rate * w[i] + (a.flash_rate if burst and i == flash else 0)) * a.tick_ms / 1000
                k = int(owed[i])
                owed[i] -= k
                for _ in range(k):
                    u, m = event(i)
                    batches[i % len(writers)][i, part(u, parts[i])].append(m)
                stats["flash" if burst and i == flash else "events"] += k
            # One batch per workspace partition per tick, each writer on its own connection.
            await asyncio.gather(*(send(c, b) for c, b in zip(writers, batches) if b))
            tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)   # never bank more than 100 ms
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            extra = f"  flash sale on {ws_name(flash)}: +{stats['flash']:,}/s" if stats["flash"] else ""
            print(f"[workspaces] {stats['events'] + stats['flash']:>7,} trigger events/s across {n} workspaces{extra}",
                  flush=True)
            stats.clear()

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--workspaces", type=int, default=200, help="brands (workspaces) on this data center")
    p.add_argument("--rate", type=float, default=10_000, help="trigger events a second, all workspaces")
    p.add_argument("--users", type=int, default=5_000_000, help="active users, all workspaces")
    p.add_argument("--writers", type=int, default=2, help="connections sending in parallel")
    p.add_argument("--tick-ms", type=float, default=20)
    p.add_argument("--flash-ws", type=int, default=2, help="workspace number that runs the flash sale (ws-0002)")
    p.add_argument("--flash-rate", type=float, default=20_000, help="extra events a second during the flash sale")
    p.add_argument("--flash-at", type=float, default=60, help="seconds before the flash sale starts")
    p.add_argument("--flash-for", type=float, default=0, help="flash sale length in seconds (0 = none)")
    asyncio.run(main(p.parse_args()))
