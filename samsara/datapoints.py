"""The datapoint writer: every updated datapoint to the stream subscription of its org and entity.

Plays the Samsara cloud side of a Data Connector: "data is streamed as it is updated on the
Samsara cloud". Each org has a fleet (long-tail sizes); a vehicle that is on reports GPS every 5 s,
one that is off every 5 minutes, as the Kafka Connector reference documents, and engine states,
fuel levels, temperatures and events (geofenceEntry, routeStopArrival, severeSpeedingStarted, ...)
arrive alongside. Each datapoint goes to the org's subscription topic for that entity, if the org
streams it, keyed by vehicle ID, with a unique id and a per-vehicle sequence per subscription, so a
connector can drop repeats and prove nothing is missing.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import (DEFAULT_URL, ENTITIES, EVENT_TYPES, STREAM, connect, ensure_topic, entities_for, msg, org_name, part,
                    partitions_for, subscriptions, vehicle_id, weights)


async def main(a):
    admin = await connect(a.url)
    n = a.orgs
    subs = subscriptions(n)
    for t, (_, _, parts) in subs.items():
        await ensure_topic(admin, STREAM, t, parts)
    print(f"[datapoints] {len(subs)} stream subscriptions ready in '{STREAM}' for {n} orgs", flush=True)
    writers = [await connect(a.url) for _ in range(a.writers)]
    w = weights(n)
    fleet = [max(5, int(a.vehicles * x)) for x in w]
    gps_rate = [v * (a.on_share / 5 + (1 - a.on_share) / 300) for v in fleet]   # GPS readings a second per org
    # org -> [(entity, topic, partitions, writer)] for every entity it streams
    routes = [[(e, f"{org_name(i)}-{e}", partitions_for(i, n), j % a.writers)
               for j, e in enumerate(entities_for(i), start=i)] for i in range(n)]
    seq = defaultdict(int)                       # (topic, vehicle) -> last sequence sent
    owed = [0.0] * n
    stats = defaultdict(int)
    dp_id = 0
    gc.freeze()                                  # keep the collector from pausing the sender

    async def send(c, batch):
        await asyncio.gather(*(c.send_messages(STREAM, t, Partitioning.partition_id(pid), ms)
                               for (t, pid), ms in batch.items()))

    async def run():
        nonlocal dp_id
        tick = time.monotonic()
        while True:
            batches = [defaultdict(list) for _ in writers]
            for i in range(n):
                owed[i] += gps_rate[i] * a.tick_ms / 1000
                k = int(owed[i])
                owed[i] -= k
                for _ in range(k):               # one GPS reading, plus whatever else this vehicle updated
                    v = vehicle_id(i, random.randrange(fleet[i]))
                    ns = time.time_ns()
                    for e, t, parts, wr in routes[i]:
                        if e != "gps" and random.random() >= ENTITIES[e]:
                            continue
                        dp_id += 1
                        seq[t, v] += 1
                        d = dict(ns=ns, id=f"{dp_id:x}", v=v, seq=seq[t, v], e=e)
                        if e == "events":
                            d["t"] = random.choice(EVENT_TYPES)
                        batches[wr][t, part(v, parts)].append(SendMessage(msg(**d)))
                        stats["streamed"] += 1
                stats["gps"] += k
            # One batch per subscription partition per tick, each writer on its own connection.
            await asyncio.gather(*(send(c, b) for c, b in zip(writers, batches) if b))
            tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)   # never bank more than 100 ms
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            print(f"[datapoints] {stats['gps']:>7,} GPS readings/s from {sum(fleet):,} vehicles in {n} orgs  "
                  f"{stats['streamed']:>7,} datapoints/s to {len(subs)} stream subscriptions", flush=True)
            stats.clear()

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--orgs", type=int, default=200, help="customer orgs with stream subscriptions on this cluster")
    p.add_argument("--vehicles", type=int, default=150_000, help="vehicles across those orgs")
    p.add_argument("--on-share", type=float, default=0.4, help="share of vehicles on (GPS every 5 s; else every 5 min)")
    p.add_argument("--writers", type=int, default=2, help="connections sending in parallel")
    p.add_argument("--tick-ms", type=float, default=20)
    asyncio.run(main(p.parse_args()))
