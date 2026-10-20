"""The source of truth: delivery partners on the road in Bengaluru, each pinging its location.

Every online partner sends a ping every --every seconds, keyed by partner ID, with a
per-partner sequence number and the position itself (never a delta). Partners take orders,
ride to the restaurant, wait, pick up and ride to the customer; each change of leg goes to
the order-legs topic. Traffic slows near Silk Board, so ETAs have something real to track.

fleet.py also reads every ETA the tracker publishes and checks it against the truth:
was it computed on the partner's latest ping, or on one the partner had already replaced?
A replaced ping on the tracking screen is the cost of a slow lane.
"""
import argparse
import asyncio
import math
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, IggyExpiry, Partitioning, PollingStrategy, SendMessage

from common import CITY, DEFAULT_URL, ETA, LEGS, PINGS, SILK_BOARD, STREAM, km, load, msg, part, partner

TICK = 0.01   # seconds


class Partner:
    __slots__ = ("id", "lat", "lon", "seq", "order", "leg", "pickup", "drop", "wait_until", "last_t", "sent")

    def __init__(self, i):
        self.id = partner(i)
        self.lat, self.lon = random.uniform(CITY[0], CITY[2]), random.uniform(CITY[1], CITY[3])
        self.seq, self.order, self.leg = 0, None, None
        self.pickup = self.drop = None
        self.wait_until, self.last_t = 0.0, time.monotonic()
        self.sent = deque(maxlen=16)          # (seq, sent_ns) of recent pings, for the ETA check


def near(lat, lon, lo_km, hi_km):
    d, a = random.uniform(lo_km, hi_km) / 111.0, random.uniform(0, 2 * math.pi)
    return (min(CITY[2], max(CITY[0], lat + d * math.sin(a))),
            min(CITY[3], max(CITY[1], lon + d * math.cos(a) / math.cos(math.radians(lat)))))


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=500))
    hours = IggyExpiry.ExpireDuration(timedelta(hours=a.keep_hours)) if a.keep_hours > 0 else None   # pings matter for hours, not days
    pings_p = await client.producer(STREAM, PINGS, topic_partitions_count=a.partitions, mode=cfg, topic_message_expiry=hours)
    legs_p = await client.producer(STREAM, LEGS, topic_partitions_count=a.partitions, mode=cfg)
    if await client.get_topic(STREAM, ETA) is None:          # track.py may not have created it yet
        try:
            await client.create_topic(STREAM, ETA, 8)
        except RuntimeError:
            pass                                             # created by track.py in the meantime
    checker = IggyClient.from_connection_string(a.url)      # its own connection, apart from the producers
    await checker.connect()
    etas = await checker.consumer_group("fleet-eta-check", STREAM, ETA, polling_strategy=PollingStrategy.Next(),
                                       batch_length=2000, auto_commit=AutoCommit.Interval(timedelta(seconds=1)),
                                       poll_interval=timedelta(microseconds=500))

    pool = [Partner(i) for i in range(int(a.partners * max(1.0, a.rush)))]
    by_id = {p.id: p for p in pool}
    slots = max(1, round(a.every / TICK))
    ring = [[] for _ in range(slots)]                        # each partner pings in its own slot, staggered
    for i, p in enumerate(pool):
        ring[i % slots].append(i)
    stats, n_orders = defaultdict(int), 0

    def speed_kmh(p):
        if km(p.lat, p.lon, *SILK_BOARD) < 1.5:
            return random.uniform(4, 9)                      # the junction everyone in Bengaluru knows
        return random.uniform(16, 28)

    def leg_event(p, ev, now_ns, **extra):
        return p.id, SendMessage(msg(k=p.id, order=p.order, ev=ev, ns=now_ns, **extra))

    def step(p, now, now_ns, legs):
        nonlocal n_orders
        dt, p.last_t = now - p.last_t, now
        if p.order is None:
            if random.random() < a.assign_p:                 # the assignment engine picks this partner
                n_orders += 1
                p.order, p.leg = f"O{n_orders:09d}", "fm"
                p.pickup = near(p.lat, p.lon, 0.3, 2.5)
                p.drop = near(*p.pickup, 1.0, 5.0)
                legs.append(leg_event(p, "assigned", now_ns, pickup=p.pickup, drop=p.drop))
            return
        if p.leg == "wt":
            if now >= p.wait_until:
                p.leg = "lm"
                legs.append(leg_event(p, "picked_up", now_ns))
            return
        tgt = p.pickup if p.leg == "fm" else p.drop
        left, move = km(p.lat, p.lon, *tgt), speed_kmh(p) * dt / 3600
        if move >= left:
            p.lat, p.lon = tgt
            if p.leg == "fm":
                p.leg, p.wait_until = "wt", now + random.uniform(*a.wait_s)
                legs.append(leg_event(p, "arrived", now_ns))
            else:
                legs.append(leg_event(p, "delivered", now_ns))
                p.order = p.leg = None
                stats["delivered"] += 1
        else:
            f = move / left
            p.lat += (tgt[0] - p.lat) * f
            p.lon += (tgt[1] - p.lon) * f

    async def send_by_partition(producer, items):
        # One batch per partition, all sent at once: per-partner order kept, about one round trip per tick.
        batches = defaultdict(list)
        for key, m in items:
            batches[part(key, a.partitions)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))

    async def ride():
        async with pings_p, legs_p:
            t0 = due = time.monotonic()
            tick = 0
            while True:
                now, now_ns = time.monotonic(), time.time_ns()
                rush = a.rush if a.rush_at and a.rush_at <= now - t0 < a.rush_at + a.rush_for else 1.0
                online = int(a.partners * rush)
                pings, legs = [], []
                for i in ring[tick % slots]:
                    p = pool[i]
                    if i >= online and p.order is None:
                        continue                             # logged off; a partner on a trip finishes it first
                    step(p, now, now_ns, legs)
                    p.seq += 1
                    p.sent.append((p.seq, now_ns))
                    pings.append((p.id, SendMessage(msg(k=p.id, seq=p.seq, lat=round(p.lat, 6), lon=round(p.lon, 6),
                                                        order=p.order, ns=now_ns))))
                stats["pings"] += len(pings)
                stats["online"] = online
                await asyncio.gather(send_by_partition(pings_p, pings), send_by_partition(legs_p, legs))
                tick += 1
                due += TICK
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def on_eta(m):
        e = load(m.payload())
        p = by_id.get(e["k"])
        if p is None:
            return
        latest = max((s for s, ns in p.sent if ns <= e["ns"]), default=e["ping_seq"])
        behind = latest - e["ping_seq"]
        stats["checked"] += 1
        stats["behind1" if behind == 1 else "behind2" if behind >= 2 else "latest"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            c = stats["checked"]
            pct = lambda k: 100 * stats[k] / c if c else 0.0
            on_trip = sum(1 for p in pool if p.order)
            print(f"[fleet] {stats['pings']:>7,} pings/s  partners online {stats['online']:,}  on a trip {on_trip:,}  "
                  f"ETAs checked {c:,}  on the latest ping {pct('latest'):6.2f}%  "
                  f"one ping behind {pct('behind1'):5.2f}%  two or more behind {pct('behind2'):5.3f}%", flush=True)
            for k in ("pings", "checked", "latest", "behind1", "behind2"):
                stats[k] = 0

    await asyncio.gather(ride(), etas.consume_messages(on_eta, asyncio.Event()), report())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--partners", type=int, default=60000, help="delivery partners online")
    ap.add_argument("--every", type=float, default=5.0, help="seconds between pings per partner")
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--assign-p", type=float, default=0.15, help="chance an idle partner gets an order at each ping")
    ap.add_argument("--wait-s", type=float, nargs=2, default=(60, 360), help="wait at the restaurant, min and max seconds")
    ap.add_argument("--keep-hours", type=float, default=6, help="ping retention on the lane")
    ap.add_argument("--rush", type=float, default=1.0, help="multiplier for partners online during the rush")
    ap.add_argument("--rush-at", type=float, default=0)
    ap.add_argument("--rush-for", type=float, default=120)
    asyncio.run(main(ap.parse_args()))
