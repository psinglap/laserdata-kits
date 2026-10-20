"""The source of truth: Porter driver-partners online in Bengaluru, two-wheelers and trucks, each pinging its location.

Every online driver sends a ping every --every seconds, keyed by driver ID, with a per-driver
sequence number and the position itself (never a delta). Drivers accept orders, drive to the
pickup, load, drive to the drop and unload. Order lifecycle events go to trip-events:
accepted (with pickup and drop), live (goods on board) and ended, the stages Porter's
order-status webhooks report.

drivers.py also knows, for every order, which ping was the first one inside 150 m of the
pickup and of the drop. It reads every arrival the trip service publishes and checks it: was it
fired on that first ping inside the fence, or one or more pings late? And how long after the
driver's phone created that ping did the notice exist? A late arrival is the cost of a slow lane:
the sender and receiver see the driver "on the way" when the driver is already at the gate.
"""
import argparse
import asyncio
import gc
import math
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, IggyExpiry, Partitioning, PollingStrategy, SendMessage

from common import (CITY, DEFAULT_URL, FENCE_KM, MILESTONES, PICKUP_AREAS, PINGS, STREAM, TRIPS, VEHICLES, Window,
                    driver, km, load, msg, part)

TICK = 0.02   # seconds; each driver still pings on its own schedule, stamped when sent
AREAS = list(PICKUP_AREAS.values())


class Driver:
    __slots__ = ("id", "veh", "lat", "lon", "seq", "order", "stage", "pickup", "drop", "until", "last_t", "speed")

    def __init__(self, i, veh):
        self.id, self.veh = driver(i), veh
        if random.random() < 0.5:                              # half wait near a market or industrial area
            self.lat, self.lon = near(*random.choice(AREAS), 0.0, 2.0)
        else:
            self.lat, self.lon = random.uniform(CITY[0], CITY[2]), random.uniform(CITY[1], CITY[3])
        self.seq, self.order, self.stage = 0, None, None
        self.pickup = self.drop = None
        self.until, self.last_t = 0.0, time.monotonic()
        self.speed = random.uniform(*VEHICLES[veh][1])


def near(lat, lon, lo_km, hi_km):
    d, a = random.uniform(lo_km, hi_km) / 111.0, random.uniform(0, 2 * math.pi)
    return (round(lat + d * math.sin(a), 6), round(lon + d * math.cos(a) / math.cos(math.radians(lat)), 6))


def vehicle_mix(n):
    out = []
    for veh, (share, _, _) in VEHICLES.items():
        out += [veh] * round(n * share)
    out += ["2W"] * (n - len(out))
    random.shuffle(out)
    return out


async def main(a):
    async def connected():
        # One connection per producer and per consumer group, so a busy one never queues behind another.
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=500))
    hours = IggyExpiry.ExpireDuration(timedelta(hours=a.keep_hours)) if a.keep_hours > 0 else None   # pings matter for hours, not days
    pings_p = await (await connected()).producer(STREAM, PINGS, topic_partitions_count=a.partitions, mode=cfg,
                                                 topic_message_expiry=hours)
    trips_p = await (await connected()).producer(STREAM, TRIPS, topic_partitions_count=a.partitions, mode=cfg)
    admin = await connected()
    if await admin.get_topic(STREAM, MILESTONES) is None:     # trips.py may not have created it yet
        try:
            await admin.create_topic(STREAM, MILESTONES, 8)
        except RuntimeError:
            pass                                               # created by trips.py in the meantime
    arrivals = await (await connected()).consumer_group("drivers-arrival-check", STREAM, MILESTONES,
                                                        polling_strategy=PollingStrategy.Next(), batch_length=2000,
                                                        auto_commit=AutoCommit.Interval(timedelta(seconds=1)),
                                                        poll_interval=timedelta(milliseconds=2))

    n_max = int(a.drivers * max(1.0, a.peak))
    pool = [Driver(i, v) for i, v in enumerate(vehicle_mix(n_max))]
    slots = max(1, round(a.every / TICK))
    ring = [[] for _ in range(slots)]                          # each driver pings in its own slot, staggered
    for i in range(len(pool)):
        ring[i % slots].append(i)
    gc.freeze()                                                # the fleet lives all run; keep it out of later GC passes
    done = set()                                               # (order, milestone) already reached
    truth = {}                                                 # (order, milestone) -> (seq, ping ns, monotonic) of the first ping inside the fence
    stats, notice, n_orders = defaultdict(int), Window(), 0

    def trip_event(d, ev, now_ns, **extra):
        return d.id, SendMessage(msg(k=d.id, order=d.order, ev=ev, v=d.veh, ns=now_ns, **extra))

    def step(d, now, now_ns, trips):
        nonlocal n_orders
        dt, d.last_t = (now - d.last_t) * a.pace, now
        if d.order is None:
            if random.random() < a.assign_p:                   # allocation offers this driver an order; accepted
                n_orders += 1
                d.order, d.stage = f"ORD{n_orders:09d}", "to_pickup"
                d.pickup = near(d.lat, d.lon, *a.pickup_km)
                d.drop = near(*d.pickup, *a.drop_km)
                trips.append(trip_event(d, "accepted", now_ns, pickup=d.pickup, drop=d.drop))
            return
        if d.stage in ("loading", "unloading"):
            if now >= d.until:
                if d.stage == "loading":
                    d.stage = "to_drop"
                    trips.append(trip_event(d, "live", now_ns))
                else:
                    trips.append(trip_event(d, "ended", now_ns))
                    d.order = d.stage = None
                    stats["ended"] += 1
            return
        tgt = d.pickup if d.stage == "to_pickup" else d.drop
        left, move = km(d.lat, d.lon, *tgt), d.speed * dt / 3600
        if move >= left:
            d.lat, d.lon = tgt
            d.stage = "loading" if d.stage == "to_pickup" else "unloading"
            d.until = now + random.uniform(*VEHICLES[d.veh][2]) * a.wait_scale / a.pace
        else:
            f = move / left
            d.lat += (tgt[0] - d.lat) * f
            d.lon += (tgt[1] - d.lon) * f

    def mark_truth(d, now, now_ns):
        # The first ping inside the fence is when the driver has reached the pickup or the drop.
        if d.order is None:
            return
        if d.stage in ("to_pickup", "loading"):
            ms, tgt = "reached_pickup", d.pickup
        else:
            ms, tgt = "reached_drop", d.drop
        key = (d.order, ms)
        if key not in done and km(d.lat, d.lon, *tgt) <= FENCE_KM:
            done.add(key)                                      # a fence is reached once per order
            truth[key] = (d.seq, now_ns, now)

    async def send_by_partition(producer, items):
        # One batch per partition, all sent at once: per-driver order kept, about one round trip per tick.
        batches = defaultdict(list)
        for key, m in items:
            batches[part(key, a.partitions)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))

    async def drive():
        async with pings_p, trips_p:
            t0 = due = time.monotonic()
            tick = 0
            while True:
                now, now_ns = time.monotonic(), time.time_ns()
                peak = a.peak if a.peak_at and a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1.0
                online = int(a.drivers * peak)
                pings, trips = [], []
                for i in ring[tick % slots]:
                    d = pool[i]
                    if i >= online and d.order is None:
                        continue                               # logged off; a driver on a trip finishes it first
                    step(d, now, now_ns, trips)
                    d.seq += 1
                    mark_truth(d, now, now_ns)
                    pings.append((d.id, SendMessage(msg(k=d.id, seq=d.seq, lat=round(d.lat, 6), lon=round(d.lon, 6),
                                                        v=d.veh, order=d.order, ns=now_ns))))
                stats["pings"] += len(pings)
                stats["online"] = online
                await asyncio.gather(send_by_partition(pings_p, pings), send_by_partition(trips_p, trips))
                tick += 1
                due += TICK
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def on_arrival(m):
        e = load(m.payload())
        t = truth.pop((e["order"], e["ms"]), None)
        if t is None:
            stats["unknown"] += 1                              # from an earlier run, or already checked
            return
        late = e["ping_seq"] - t[0]
        stats["checked"] += 1
        stats["first" if late <= 0 else "late1" if late == 1 else "late2"] += 1
        notice.add((time.time_ns() - t[1]) / 1e6)

    async def report():
        while True:
            await asyncio.sleep(1)
            now = time.monotonic()
            for key in [k for k, t in truth.items() if now - t[2] > a.overdue_s]:
                truth.pop(key)
                stats["missed"] += 1                           # the driver reached the fence; no notice in time
            c, n = stats["checked"], notice.flush()
            pct = lambda k: 100 * stats[k] / c if c else 0.0
            on_trip = sum(1 for d in pool if d.order)
            print(f"[drivers] {stats['pings']:>7,} pings/s  online {stats['online']:,}  on a trip {on_trip:,}  "
                  f"arrivals checked {c:,}  on the first ping inside the fence {pct('first'):6.2f}%  "
                  f"one ping late {pct('late1'):5.2f}%  two or more late {pct('late2'):5.3f}%  "
                  f"missed {stats['missed']:,}  ping-to-notice p99 {n['p99']:6.1f}ms", flush=True)
            for k in ("pings", "checked", "first", "late1", "late2", "missed", "unknown"):
                stats[k] = 0

    await asyncio.gather(drive(), arrivals.consume_messages(on_arrival, asyncio.Event()), report())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--drivers", type=int, default=20000, help="driver-partners online in the city")
    ap.add_argument("--every", type=float, default=5.0, help="seconds between pings per driver")
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--assign-p", type=float, default=0.02, help="chance an idle driver accepts an order at each ping")
    ap.add_argument("--pickup-km", type=float, nargs=2, default=(0.5, 4.0), help="driver to pickup, min and max km")
    ap.add_argument("--drop-km", type=float, nargs=2, default=(1.0, 12.0), help="pickup to drop, min and max km")
    ap.add_argument("--wait-scale", type=float, default=1.0, help="multiplier on loading and unloading times")
    ap.add_argument("--pace", type=float, default=1.0, help="road time runs this many times faster (CI uses 3); pings stay real-time")
    ap.add_argument("--overdue-s", type=float, default=10, help="an arrival with no notice after this long counts as missed")
    ap.add_argument("--keep-hours", type=float, default=6, help="ping retention on the lane")
    ap.add_argument("--peak", type=float, default=1.0, help="multiplier for drivers online during the peak")
    ap.add_argument("--peak-at", type=float, default=0)
    ap.add_argument("--peak-for", type=float, default=120)
    asyncio.run(main(ap.parse_args()))
