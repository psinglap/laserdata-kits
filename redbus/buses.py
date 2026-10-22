"""The source of truth: buses on live journeys leaving the city, each sending GPS packets, keyed by journey.

Every bus on a tracked journey sends a GPS packet every --every seconds, keyed by SVC_DOJ (ServiceNo + date of
journey), with a per-journey sequence and the position itself (never a delta). Before its first packet a journey's
assignment goes on the same key: its boarding points in route order and how many passengers board at each, as
the assignment validator and the passenger data would give them. The bus drives from point to point, halts at
each for a few minutes, then heads out of town; the journey ends and a new departure takes its place.

buses.py also runs the boarding-point rules (common.bp_step) on every packet it sends, so it knows which packet
caused each ENTRY, HALT and EXIT. It reads every boarding-point event and every passenger alert the tracker
publishes and checks them: was the event published from the right packet, or late? Did every passenger boarding
there get the "bus is arriving" alert, and how long after the packet was created did it exist? A late or missing
alert is the cost of a slow lane: a traveller at a bus stop who does not know the bus is already there.
"""
import argparse
import asyncio
import gc
import math
import random
import time
from collections import defaultdict
from datetime import date, timedelta

from apache_iggy import DirectProducerConfig, IggyClient, IggyExpiry, Partitioning, SendMessage

from common import (ALERTS, BP_EVENTS, CITIES, DESTS, GPS, PARTITIONS, STREAM, DEFAULT_URL, Journey, Window, bp_step,
                    ensure_topics, km, load, msg, part, read_partitions, svc_doj)

TICK = 0.02   # seconds; each bus still sends on its own schedule, stamped when sent


class Bus:
    __slots__ = ("k", "j", "lat", "lon", "seq", "leg", "stage", "until", "speed", "last_t", "out", "active")

    def __init__(self):
        self.k, self.j, self.active = None, None, False


def new_journey(b, n, a, now, first):
    src = random.choice(list(CITIES))
    dep = f"{random.choice(range(17, 24)):02d}{random.choice((0, 15, 30, 45)):02d}"   # evening departures
    b.k = svc_doj(random.randrange(1, 4000), src, random.choice(DESTS[src]), dep, date.today().strftime("%Y%m%d"))
    b.k += f"-{n}"                                      # unique per simulated departure
    pts = CITIES[src]
    start = random.randrange(0, len(pts) - 1)           # services join the route at different points
    bps = [[name, lat, lon, random.choice((0, 1, 2, 3, 4, 6, 8, 10))] for name, lat, lon in pts[start:]]
    b.j = Journey(bps)
    b.seq, b.leg, b.speed, b.last_t = 0, 0, random.uniform(*a.speed_kmh), now
    (lat0, lon0), (lat1, lon1) = bps[-2][1:3] if len(bps) > 1 else bps[0][1:3], bps[-1][1:3]
    f = 2.5 / max(0.1, km(lat0, lon0, lat1, lon1))     # out of town: 2.5 km past the last point, same heading
    b.out = (lat1 + (lat1 - lat0) * f, lon1 + (lon1 - lon0) * f)
    if first:
        # at start-up the fleet is already on the road: somewhere before the first point, or halted at it
        lat, lon = bps[0][1], bps[0][2]
        d, ang = random.uniform(0.0, 3.0), random.uniform(0, 2 * math.pi)
        b.lat, b.lon = lat + d / 111 * math.sin(ang), lon + d / 111 * math.cos(ang)
    else:
        b.lat, b.lon = pts[start - 1][1:3] if start else (bps[0][1] - 0.02, bps[0][2] - 0.02)
    b.stage, b.until, b.active = "drive", 0.0, True
    return [[n, la, lo, p] for n, la, lo, p in bps]


def step(b, now, a):
    dt, b.last_t = (now - b.last_t) * a.pace, now
    if b.stage == "halt":
        if now >= b.until:
            b.stage, b.leg = "drive", b.leg + 1
        return False
    tgt = b.j.bps[b.leg][1:3] if b.leg < len(b.j.bps) else b.out
    left, move = km(b.lat, b.lon, *tgt), b.speed * dt / 3600
    if move >= left:
        b.lat, b.lon = tgt
        if b.leg >= len(b.j.bps):
            return True                                 # out of town: the journey leaves the city lane
        b.stage, b.until = "halt", now + random.uniform(*a.halt_min) * 60 / a.pace
    else:
        f = move / left
        b.lat += (tgt[0] - b.lat) * f
        b.lon += (tgt[1] - b.lon) * f
    return False


async def main(a):
    async def connected():
        # One connection per producer and per reader, so a busy one never queues behind another.
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await connected(), GPS, BP_EVENTS, ALERTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=500))
    keep = IggyExpiry.ExpireDuration(timedelta(hours=a.keep_hours)) if a.keep_hours > 0 else None
    gps_p = await (await connected()).producer(STREAM, GPS, topic_partitions_count=PARTITIONS[GPS], mode=cfg,
                                               topic_message_expiry=keep)
    n_max = int(a.journeys * max(1.0, a.peak))
    pool = [Bus() for _ in range(n_max)]
    slots = max(1, round(a.every / TICK))
    ring = [[] for _ in range(slots)]                   # each bus sends in its own slot, staggered
    for i in range(n_max):
        ring[i % slots].append(i)
    truth = {}                                          # (journey, bp, ev) -> (packet seq, packet ns, monotonic)
    waiting = {}                                        # (journey, bp) -> [alerts still due, packet ns, monotonic]
    stats, alert_lat, n_dep = defaultdict(int), Window(), [0]
    gc.freeze()

    def on_event(m):
        e = load(m.payload())
        t = truth.pop((e["k"], e["bp"], e["ev"]), None)
        if t is None:
            stats["unknown"] += 1                       # from an earlier run, or already checked
            return
        stats["checked"] += 1
        stats["right" if e["seq"] == t[0] else "late"] += 1

    def on_alert(m):
        e = load(m.payload())
        w = waiting.get((e["k"], e["bp"]))
        if w is None:
            stats["unknown_alerts"] += 1
            return
        stats["alerted"] += 1
        alert_lat.add((time.time_ns() - w[1]) / 1e6)
        w[0] -= 1
        if w[0] <= 0:
            del waiting[(e["k"], e["bp"])]

    async def send_by_partition(items):
        # One batch per partition, all sent at once: per-journey order kept, about one round trip per tick.
        batches = defaultdict(list)
        for key, m in items:
            batches[part(key, PARTITIONS[GPS])].append(m)
        await asyncio.gather(*(gps_p.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))

    async def drive():
        async with gps_p:
            t0 = due = time.monotonic()
            tick = 0
            while True:
                now, now_ns = time.monotonic(), time.time_ns()
                peak = a.peak if a.peak_at and a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1.0
                tracked = int(a.journeys * peak)
                out = []
                for i in ring[tick % slots]:
                    b = pool[i]
                    if not b.active:
                        if i >= tracked:
                            continue                    # not departed; a journey already on the road finishes
                        n_dep[0] += 1
                        bps = new_journey(b, n_dep[0], a, now, first=(tick < slots))
                        out.append((b.k, SendMessage(msg(t="assign", k=b.k, bps=bps, ns=now_ns))))
                    if step(b, now, a):
                        out.append((b.k, SendMessage(msg(t="end", k=b.k, ns=now_ns))))
                        b.active = False
                        stats["ended"] += 1
                        continue
                    b.seq += 1
                    lat, lon = round(b.lat, 6), round(b.lon, 6)
                    for ev, bp in bp_step(b.j, lat, lon):  # the position as the packet carries it
                        truth[(b.k, bp, ev)] = (b.seq, now_ns, now)
                        pax = b.j.bps[bp][3]
                        if ev == "ENTRY" and pax:
                            waiting[(b.k, bp)] = [pax, now_ns, now]
                            stats["due"] += pax
                    out.append((b.k, SendMessage(msg(t="gps", k=b.k, seq=b.seq, lat=lat, lon=lon, ns=now_ns))))
                    stats["packets"] += 1
                await send_by_partition(out)
                tick += 1
                due += TICK                             # scheduled from its due time, so one slow tick doesn't drift
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            gc.freeze()                                 # journeys and the truth table live long: out of GC's reach
            now = time.monotonic()
            for key in [k for k, t in truth.items() if now - t[2] > a.overdue_s]:
                truth.pop(key)
                stats["missed"] += 1                    # the bus crossed the fence; no event in time
            for key in [k for k, w in waiting.items() if now - w[2] > a.overdue_s]:
                stats["not_alerted"] += waiting.pop(key)[0]
            c, r = stats["checked"], alert_lat.flush()
            stats["tracked"] = sum(1 for b in pool if b.active)
            pct = lambda k: 100 * stats[k] / c if c else 0.0
            print(f"[buses] {stats['packets']:>6,} packets/s  journeys tracked {stats['tracked']:,}  "
                  f"bp events checked {c:,}  from the right packet {pct('right'):6.2f}%  late {pct('late'):5.2f}%  "
                  f"missed {stats['missed']:,}  passengers alerted {stats['alerted']:,}  "
                  f"not alerted {stats['not_alerted']:,}  packet-to-alert p99 {r['p99']:6.1f}ms", flush=True)
            for k in ("packets", "checked", "right", "late", "missed", "alerted", "not_alerted", "unknown",
                      "unknown_alerts", "due", "ended"):
                stats[k] = 0

    readers = [read_partitions(await connected(), BP_EVENTS, list(range(PARTITIONS[BP_EVENTS])), on_event, "buses-ev"),
               read_partitions(await connected(), ALERTS, list(range(PARTITIONS[ALERTS])), on_alert, "buses-al")]
    await asyncio.gather(drive(), report(), *readers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--journeys", type=int, default=15000, help="journeys tracked at once")
    ap.add_argument("--every", type=float, default=45.0, help="seconds between GPS packets per bus")
    ap.add_argument("--speed-kmh", type=float, nargs=2, default=(15, 30), help="city road speed, min and max")
    ap.add_argument("--halt-min", type=float, nargs=2, default=(2, 8), help="minutes a bus waits at a boarding point")
    ap.add_argument("--pace", type=float, default=1.0, help="road time runs this many times faster (CI uses 15); packets stay real-time")
    ap.add_argument("--overdue-s", type=float, default=10, help="an event or alert not seen after this long counts as missed")
    ap.add_argument("--keep-hours", type=float, default=24, help="packet retention on the lane, for replay and simulation")
    ap.add_argument("--peak", type=float, default=1.0, help="multiplier for journeys tracked during the peak")
    ap.add_argument("--peak-at", type=float, default=0)
    ap.add_argument("--peak-for", type=float, default=120)
    asyncio.run(main(ap.parse_args()))
