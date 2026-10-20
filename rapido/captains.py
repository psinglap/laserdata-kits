"""The source of truth: on-duty captains in Hyderabad pinging their location, and customers requesting rides.

Every on-duty captain sends a ping every --every seconds, keyed by captain ID, with a
per-captain sequence number, the position itself (never a delta) and whether the captain
is free or on a ride. Customers request rides at --rpm a minute, most of them from the
office and college hotspots. A captain who receives an offer rides to the pickup, waits
for the customer, rides to the drop and is free again.

captains.py also reads every offer the matcher publishes and checks it against the truth:
was it computed on the captain's latest ping, or on one the captain had already replaced?
And how far off was the pickup distance the matcher used? An offer built on an old
position is a captain further away than the matcher thought.
"""
import argparse
import asyncio
import math
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, IggyExpiry, Partitioning, PollingStrategy, SendMessage

from common import CITY, DEFAULT_URL, HOTSPOTS, OFFERS, PINGS, RIDES, STREAM, captain, km, load, msg, part

TICK = 0.02            # seconds; each captain still pings on its own schedule, stamped when sent
SPOTS = list(HOTSPOTS.values())


def near(lat, lon, lo_km, hi_km):
    d, a = random.uniform(lo_km, hi_km) / 111.0, random.uniform(0, 2 * math.pi)
    return (min(CITY[2], max(CITY[0], lat + d * math.sin(a))),
            min(CITY[3], max(CITY[1], lon + d * math.cos(a) / math.cos(math.radians(lat)))))


def anywhere():
    return random.uniform(CITY[0], CITY[2]), random.uniform(CITY[1], CITY[3])


class Captain:
    __slots__ = ("id", "lat", "lon", "seq", "ride", "leg", "pickup", "drop", "goal", "wait_until", "last_t", "sent")

    def __init__(self, i):
        self.id = captain(i)
        self.lat, self.lon = near(*random.choice(SPOTS), 0.2, 6.0) if random.random() < 0.6 else anywhere()
        self.seq, self.ride, self.leg = 0, None, None
        self.pickup = self.drop = None
        self.goal = near(*random.choice(SPOTS), 0.2, 3.0)    # free captains drift towards demand
        self.wait_until, self.last_t = 0.0, time.monotonic()
        self.sent = deque(maxlen=16)                           # (seq, sent_ns) of recent pings, for the offer check


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=500))
    hours = IggyExpiry.ExpireDuration(timedelta(hours=a.keep_hours)) if a.keep_hours > 0 else None   # pings matter for hours
    pings_p = await client.producer(STREAM, PINGS, topic_partitions_count=a.partitions, mode=cfg, topic_message_expiry=hours)
    rides_client = IggyClient.from_connection_string(a.url)     # one connection per producer
    await rides_client.connect()
    rides_p = await rides_client.producer(STREAM, RIDES, topic_partitions_count=4, mode=cfg)
    if await client.get_topic(STREAM, OFFERS) is None:          # match.py may not have created it yet
        try:
            await client.create_topic(STREAM, OFFERS, 8)
        except RuntimeError:
            pass                                                # created by match.py in the meantime
    checker = IggyClient.from_connection_string(a.url)          # its own connection, apart from the producers
    await checker.connect()
    offers = await checker.consumer_group("captains-offer-check", STREAM, OFFERS, polling_strategy=PollingStrategy.Next(),
                                         batch_length=2000, auto_commit=AutoCommit.Interval(timedelta(seconds=1)),
                                         poll_interval=timedelta(milliseconds=5))   # a check, not on the lane's clock

    pool = [Captain(i) for i in range(a.captains)]
    by_id = {c.id: c for c in pool}
    slots = max(1, round(a.every / TICK))
    ring = [[] for _ in range(slots)]                            # each captain pings in its own slot, staggered
    for i, c in enumerate(pool):
        ring[i % slots].append(i)
    stats, n_rides = defaultdict(int), 0

    def speed_kmh(c, peak):
        if peak > 1 and km(c.lat, c.lon, *HOTSPOTS["HITEC City"]) < 2.0:
            return random.uniform(8, 14)                         # office peak around HITEC City
        return random.uniform(10, 18) if c.ride is None else random.uniform(18, 30)

    def step(c, now, peak):
        dt, c.last_t = now - c.last_t, now
        if c.leg == "wait":
            if now >= c.wait_until:
                c.leg = "trip"
            return
        tgt = c.goal if c.ride is None else c.pickup if c.leg == "to_pickup" else c.drop
        left, move = km(c.lat, c.lon, *tgt), speed_kmh(c, peak) * dt / 3600
        if move >= left:
            c.lat, c.lon = tgt
            if c.ride is None:
                c.goal = near(*random.choice(SPOTS), 0.2, 3.0) if random.random() < 0.7 else anywhere()
            elif c.leg == "to_pickup":
                c.leg, c.wait_until = "wait", now + random.uniform(*a.wait_s)
            else:
                c.ride = c.leg = None                            # dropped; free again
                stats["completed"] += 1
        else:
            f = move / left
            c.lat += (tgt[0] - c.lat) * f
            c.lon += (tgt[1] - c.lon) * f

    async def send_by_partition(producer, items, partitions):
        # One batch per partition, all sent at once: per-key order kept, about one round trip per tick.
        batches = defaultdict(list)
        for key, m in items:
            batches[part(key, partitions)].append(m)
        await asyncio.gather(*(
            producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
            for pid, ms in batches.items()))

    async def run():
        nonlocal n_rides
        async with pings_p, rides_p:
            t0 = due = time.monotonic()
            tick, owed = 0, 0.0
            while True:
                now, now_ns = time.monotonic(), time.time_ns()
                peak = a.peak if a.peak_at and a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1.0
                pings, rides = [], []
                for i in ring[tick % slots]:
                    c = pool[i]
                    step(c, now, peak)
                    c.seq += 1
                    c.sent.append((c.seq, now_ns))
                    pings.append((c.id, SendMessage(msg(k=c.id, seq=c.seq, lat=round(c.lat, 6), lon=round(c.lon, 6),
                                                        st="free" if c.ride is None else "busy", ride=c.ride, ns=now_ns))))
                owed += a.rpm * peak / 60 * TICK
                while owed >= 1:                                 # customers tapping "request"
                    owed -= 1
                    n_rides += 1
                    rid = f"R{n_rides:010d}"
                    pick = near(*random.choice(SPOTS), 0.1, 2.5) if random.random() < a.hotspot_share else anywhere()
                    drop = near(*pick, 2.0, 10.0)
                    rides.append((rid, SendMessage(msg(ride=rid, pickup=[round(pick[0], 6), round(pick[1], 6)],
                                                       drop=[round(drop[0], 6), round(drop[1], 6)], ns=now_ns))))
                stats["pings"] += len(pings)
                stats["requests"] += len(rides)
                stats["peak"] = peak
                await asyncio.gather(send_by_partition(pings_p, pings, a.partitions), send_by_partition(rides_p, rides, 4))
                tick += 1
                due += TICK
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def on_offer(m):
        o = load(m.payload())
        c = by_id.get(o.get("k"))
        if c is None:
            return
        latest = max((s for s, ns in c.sent if ns <= o["ns"]), default=o["ping_seq"])
        behind = latest - o["ping_seq"]
        true_km = km(c.lat, c.lon, *o["pickup"])
        stats["checked"] += 1
        stats["behind1" if behind == 1 else "behind2" if behind >= 2 else "latest"] += 1
        if abs(true_km - o["d_km"]) > 0.25:
            stats["off250"] += 1
        if c.ride is not None:
            stats["busy"] += 1                                   # offered a captain already on a ride
            return
        c.ride, c.leg, c.pickup, c.drop = o["ride"], "to_pickup", tuple(o["pickup"]), tuple(o["drop"])

    async def report():
        while True:
            await asyncio.sleep(1)
            n = stats["checked"]
            pct = lambda k: 100 * stats[k] / n if n else 0.0
            on_ride = sum(1 for c in pool if c.ride)
            print(f"[captains] {stats['pings']:>7,} pings/s  on duty {len(pool):,}  on a ride {on_ride:,}  "
                  f"requests {stats['requests']:,}/s{'  PEAK x%g' % stats['peak'] if stats['peak'] > 1 else ''}  "
                  f"offers checked {n:,}  on the latest ping {pct('latest'):6.2f}%  one ping behind {pct('behind1'):5.2f}%  "
                  f"two or more behind {pct('behind2'):5.3f}%  pickup distance off >250 m {pct('off250'):5.3f}%  "
                  f"offered while busy {stats['busy']}", flush=True)
            for k in ("pings", "requests", "checked", "latest", "behind1", "behind2", "off250", "busy"):
                stats[k] = 0

    await asyncio.gather(run(), offers.consume_messages(on_offer, asyncio.Event()), report())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--captains", type=int, default=50000, help="captains on duty")
    ap.add_argument("--every", type=float, default=5.0, help="seconds between pings per captain")
    ap.add_argument("--rpm", type=float, default=2000, help="ride requests per minute in this city")
    ap.add_argument("--hotspot-share", type=float, default=0.7, help="share of requests from the office and college hotspots")
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--wait-s", type=float, nargs=2, default=(30, 120), help="wait at the pickup, min and max seconds")
    ap.add_argument("--keep-hours", type=float, default=6, help="ping retention on the lane")
    ap.add_argument("--peak", type=float, default=1.0, help="multiplier for ride requests during the peak")
    ap.add_argument("--peak-at", type=float, default=0)
    ap.add_argument("--peak-for", type=float, default=120)
    asyncio.run(main(ap.parse_args()))
