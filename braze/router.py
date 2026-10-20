"""The connector router: every engagement event to the topic of each Currents connector configured for it.

Plays the step Braze's Kafka talk describes as a routing app reading the event-type topics and
writing integration-specific topics, with a configuration that says which events each
integration exports. Here each connector has its own Apache Iggy topic, keyed by user ID, and
every message carries the event's unique id (as Currents events do) and a per-user sequence
number per connector, so a connector can drop repeats and prove nothing is missing.

App sizes follow a long tail (a few very large apps, many small ones).
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import (DEFAULT_URL, EVENTS, KINDS, STREAM, app_name, connect, connectors, ensure_topic, kinds_for, msg,
                    part, partitions_for, weights)


async def main(a):
    admin = await connect(a.url)
    n = a.apps
    conns = connectors(n)
    for t, (_, _, parts) in conns.items():
        await ensure_topic(admin, STREAM, t, parts)
    print(f"[router] {len(conns)} connector topics ready in '{STREAM}' for {n} apps", flush=True)
    writers = [await connect(a.url) for _ in range(a.writers)]
    w = weights(n)
    users = [max(200, int(a.users * x)) for x in w]
    names, shares = zip(*EVENTS)
    # app -> [(topic, partitions, event types it exports, writer)]
    routes = [[(f"{app_name(i)}-{k}", partitions_for(i, n), KINDS[k], j % a.writers)
               for j, k in enumerate(kinds_for(i), start=i)] for i in range(n)]
    seq = defaultdict(int)                       # (topic, user) -> last sequence sent
    owed = [0.0] * n
    stats = defaultdict(int)
    event_id = 0
    gc.freeze()                                  # keep the collector from pausing the sender

    async def send(c, batch):
        await asyncio.gather(*(c.send_messages(STREAM, t, Partitioning.partition_id(pid), ms)
                               for (t, pid), ms in batch.items()))

    async def run():
        nonlocal event_id
        tick = time.monotonic()
        while True:
            batches = [defaultdict(list) for _ in writers]
            for i in range(n):
                owed[i] += a.rate * w[i] * a.tick_ms / 1000
                k = int(owed[i])
                owed[i] -= k
                for _ in range(k):
                    u = f"u{random.randrange(users[i]):08d}"
                    e = random.choices(names, shares)[0]
                    event_id += 1
                    ns = time.time_ns()
                    for t, parts, wanted, wr in routes[i]:
                        if e in wanted:          # this connector is configured to export this event type
                            seq[t, u] += 1
                            batches[wr][t, part(u, parts)].append(
                                SendMessage(msg(ns=ns, id=f"{event_id:x}", u=u, seq=seq[t, u], e=e)))
                            stats["exported"] += 1
                stats["events"] += k
            # One batch per connector partition per tick, each writer on its own connection.
            await asyncio.gather(*(send(c, b) for c, b in zip(writers, batches) if b))
            tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)   # never bank more than 100 ms
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            print(f"[router] {stats['events']:>7,} engagement events/s from {n} apps  "
                  f"{stats['exported']:>7,} routed to {len(conns)} connector topics", flush=True)
            stats.clear()

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--apps", type=int, default=150, help="customer apps with Currents connectors on this cluster")
    p.add_argument("--rate", type=float, default=12_000, help="engagement events a second, all apps, before fan-out")
    p.add_argument("--users", type=int, default=20_000_000, help="active users, all apps")
    p.add_argument("--writers", type=int, default=2, help="connections sending in parallel")
    p.add_argument("--tick-ms", type=float, default=20)
    asyncio.run(main(p.parse_args()))
