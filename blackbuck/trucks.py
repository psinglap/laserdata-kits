"""A fleet of trucks with GPS devices and capacitive fuel sensors, and the notifiers that read every fuel alert.

Plays --trucks trucks in one process. Each truck's GPS device publishes a ping every --ping-s seconds to
fuel-pings, keyed by device, as the ingestion layer would after formatting it: fuel level in litres, ignition,
speed, position and the device's ping sequence. Trucks alternate between driving and parking (ignition off).
While parked, some are refuelled and a few are drained: the fuel level falls at a siphoning rate for a few
minutes. Readings carry illustrative sensor noise (a small jitter every reading, slosh while driving, and an
occasional single-reading spike), so the rules must tell a drain from noise and from a refuel.
Durations and rates are illustrative; set them to yours with the flags.

--notifiers readers play the services that act on fuel-alerts (the owner's app push, the fleet dashboard, SMS),
each on its own connection, reading the alerts partition directly from its own position.

The business numbers: of the drain alerts that reached the owner app, how many arrived while the fuel was still
being drained, and how many litres were still in the tank then (what the owner can still save by calling the
driver or the nearest dhaba). The same figures for an alert that arrived --compare-s later, and the drain alerts
raised for trucks that were not being drained (false alarms, which must stay at zero). Printed every second, with
ping-to-notifier p50/p99 (the whole loop: device ping, fuel rules, alert, notifier).
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ALERTS, DEFAULT_URL, DRAIN, PARTITIONS, PINGS, REFUEL, STREAM, Stalls, Window, device,
                    ensure_topics, load, msg, part)

TANK = 400.0                        # litres (illustrative; trucks carry 200-600 L tanks)
BURN = 0.005                        # litres per second while driving (about 0.3 L a minute)
NOTIFIERS = ["owner-app", "fleet-dashboard", "sms", "support-desk", "partner-webhooks", "reports"]


class Truck:
    __slots__ = ("d", "seq", "level", "ign", "spd", "end", "t", "lat", "lon",
                 "d_start", "d_end", "d_rate", "last_drain", "r_start", "r_end", "r_rate", "last_refuel")

    def __init__(self, d, now, a):
        self.d, self.seq, self.t = d, 0, now
        self.level = random.uniform(80, TANK - 20)
        r = random.Random(d)
        self.lat, self.lon = r.uniform(12, 28), r.uniform(73, 86)      # somewhere on India's highways
        self.d_start = self.d_end = self.r_start = self.r_end = 0.0
        self.d_rate = self.r_rate = 0.0
        self.last_drain = self.last_refuel = -1e9
        if random.random() < a.parked_share:
            self.park(now - random.uniform(0, 600), a)
        else:
            self.drive(now - random.uniform(0, 600), a)

    def park(self, now, a):
        self.ign, self.spd = 0, 0
        self.end = now + random.uniform(*a.stop_min) * 60

    def drive(self, now, a):
        self.ign, self.spd = 1, random.randint(25, 60)
        self.end = now + random.uniform(*a.drive_min) * 60

    def draining(self, now):
        return self.d_start <= now < self.d_end

    def advance(self, now, a):
        """True fuel level and state since the last ping."""
        prev, self.t = self.t, now
        if self.ign:
            self.level = max(0.0, self.level - BURN * (now - prev))
            self.lat += random.uniform(-0.002, 0.002)
            self.lon += random.uniform(-0.002, 0.002)
            if now >= self.end:
                self.park(now, a)
            return
        if self.d_end > prev:               # a drain running in this interval
            dt = max(0.0, min(now, self.d_end) - max(prev, self.d_start))
            self.level = max(0.0, self.level - self.d_rate * dt)
            if now >= self.d_end:
                self.last_drain = self.d_end
        if self.r_end > prev:               # a refuel running in this interval
            dt = max(0.0, min(now, self.r_end) - max(prev, self.r_start))
            self.level = min(TANK, self.level + self.r_rate * dt)
            if now >= self.r_end:
                self.last_refuel = self.r_end
        if now >= self.end and now >= self.d_end and now >= self.r_end:
            self.drive(now, a)

    def reading(self):
        """What the sensor reports: the true level plus jitter, slosh while driving, and now and then a spike."""
        v = self.level + random.gauss(0, 3.0 if self.ign else 0.8)
        if random.random() < 0.01:
            v += random.choice((-1, 1)) * random.uniform(5, 10)
        return round(max(0.0, v), 1)


def start_drain(s, now, ago=0.0):
    """Someone starts siphoning this parked truck's tank (rate and length illustrative)."""
    s.d_rate = random.uniform(10, 25) / 60                    # litres per second
    dur = min(random.uniform(2, 10) * 60, max(0.0, s.level - 20) / s.d_rate)
    s.d_start, s.d_end = now - ago, now - ago + dur
    s.end = max(s.end, s.d_end + random.uniform(30, 300))   # the truck stays parked past the drain


def start_refuel(s, now):
    s.r_rate = 40 / 60                                       # litres per second at the pump
    s.r_start = now
    s.r_end = now + min(TANK - s.level, random.uniform(100, 250)) / s.r_rate
    s.end = max(s.end, s.r_end + random.uniform(60, 300))


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), PINGS, ALERTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, PINGS, topic_partitions_count=PARTITIONS[PINGS], mode=cfg)
    start_ns = time.time_ns()
    now = time.monotonic()
    trucks = [Truck(device(i), now, a) for i in range(a.trucks)]
    by_d = {s.d: s for s in trucks}
    pid = [part(s.d, PARTITIONS[PINGS]) for s in trucks]
    # Start mid-shift: drains already running on some parked trucks, started a while ago.
    parked = [s for s in trucks if not s.ign]
    for s in random.sample(parked, min(len(parked), int(a.drains_per_min * 6))):
        start_drain(s, now, ago=random.uniform(0, 300))
    due = [(now + random.uniform(0, a.ping_s), i) for i in range(a.trucks)]
    heapq.heapify(due)
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    seen = set()                            # (device, kind, ping seq) the owner app already has

    def reader(name):
        def on_alert(m):
            d = load(m.payload())
            if d["src_ns"] > start_ns:
                lat.add((time.time_ns() - d["src_ns"]) / 1e6)   # live alerts only, not ones replayed at start-up
            if name != "owner-app" or (d["d"], d["kind"], d["ep"]) in seen:
                return                      # repeated alert (at-least-once): already counted
            seen.add((d["d"], d["kind"], d["ep"]))
            s, t = by_d[d["d"]], time.monotonic()
            if d["kind"] == REFUEL:
                stats["refuels"] += 1
                if not (s.r_end > t or t - s.last_refuel < 900):
                    stats["false_refuel"] += 1
                return
            stats["drain_alerts"] += 1
            src = t - (time.time_ns() - d["src_ns"]) / 1e9     # when the ping that raised it was taken
            if s.draining(t):
                stats["while_draining"] += 1        # the owner can still call the driver or the dhaba
                stats["litres_left"] += s.d_rate * (s.d_end - t)
                if s.d_end - src > a.compare_s:     # a lane this much slower would still have found it running
                    stats["at_compare"] += 1
                    stats["litres_compare"] += s.d_rate * (s.d_end - src - a.compare_s)
            elif t - s.last_drain < 900:
                stats["late"] += 1                  # the drain had already ended when the alert arrived
                if src < s.last_drain:
                    stats["late_lane"] += 1         # ... though its ping was taken while it ran: the lane's delay
            else:
                stats["false_drain"] += 1           # no drain on this truck: a false alarm
        return on_alert

    async def notify(name):
        # One connection per notifier, reading the alerts partition directly from its own position (no consumer
        # group, nothing stored on the broker per poll); a full batch means more is waiting, so no pause.
        c, cb = await own_client(), reader(name)
        who, nxt = Consumer.Single(f"{name}-{os.getpid()}"), None
        while True:
            ms = await c.poll_messages(STREAM, ALERTS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    cb(m)
            if len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def pick_parked(busy):
        for _ in range(20):
            s = trucks[random.randrange(a.trucks)]
            if not s.ign and not busy(s):
                return s
        return None

    async def device_loop():
        t0 = tick = last = time.monotonic()
        owed_d = owed_r = 0.0
        async with producer:
            while True:
                now = time.monotonic()
                x = a.peak if a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1
                owed_d += x * a.drains_per_min / 60 * (now - last)
                owed_r += a.refuels_per_min / 60 * (now - last)
                last = now
                while owed_d >= 1:
                    owed_d -= 1
                    s = pick_parked(lambda s: s.draining(now) or s.r_end > now)
                    if s:
                        start_drain(s, now)
                        stats["drains"] += 1
                while owed_r >= 1:
                    owed_r -= 1
                    s = pick_parked(lambda s: s.d_end > now or s.r_end > now)
                    if s:
                        start_refuel(s, now)
                batches = defaultdict(list)
                while due and due[0][0] <= now:
                    t, i = heapq.heappop(due)
                    heapq.heappush(due, (t + a.ping_s, i))   # from its due time, so a late tick never bunches pings
                    s = trucks[i]
                    s.advance(now, a)
                    s.seq += 1
                    stats["pings"] += 1
                    stats["parked"] += not s.ign
                    batches[pid[i]].append(SendMessage(msg(
                        d=s.d, seq=s.seq, fuel=s.reading(), ign=s.ign, spd=s.spd,
                        lat=round(s.lat, 5), lon=round(s.lon, 5))))
                # One batch per partition, all sent at once: per-device order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["peak"] = x > 1
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[trucks] {a.trucks:,} trucks, a ping every {a.ping_s:g} s, {a.drains_per_min:g} drains and "
              f"{a.refuels_per_min:g} refuels a minute across the fleet, {a.notifiers} notifiers", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "night halt" if stats["peak"] else "steady"
            n = int(stats["drain_alerts"])
            print(f"[trucks] {int(stats['pings']):>6,} pings/s ({phase})  parked {int(stats['parked']):,}  "
                  f"drains started {int(stats['drains']):,}  drain alerts at the owner app {n:,}, while still "
                  f"draining {int(stats['while_draining']):,} (at {a.compare_s:g} s it would be "
                  f"{int(stats['at_compare']):,})  litres still in the tank {stats['litres_left']:,.0f} "
                  f"(at {a.compare_s:g} s {stats['litres_compare']:,.0f})  after the drain ended "
                  f"{int(stats['late']):,} (lane delay {int(stats['late_lane']):,})  false drain alerts {int(stats['false_drain']):,}  "
                  f"refuels recorded {int(stats['refuels']):,} (false {int(stats['false_refuel']):,})  "
                  f"ping-to-notifiers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  {stalls.read()}",
                  flush=True)
            stats.clear()

    await asyncio.gather(device_loop(), report(), stalls.watch(), *(notify(n) for n in NOTIFIERS[: a.notifiers]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--trucks", type=int, default=50_000, help="trucks with a GPS device and a fuel sensor")
    p.add_argument("--ping-s", type=float, default=10, help="seconds between pings per device")
    p.add_argument("--parked-share", type=float, default=0.5, help="share of trucks parked at the start")
    p.add_argument("--drive-min", type=float, nargs=2, default=(20, 120), help="minutes driving between stops")
    p.add_argument("--stop-min", type=float, nargs=2, default=(10, 90), help="minutes parked at a stop")
    p.add_argument("--drains-per-min", type=float, default=6,
                   help="drains starting a minute across the fleet (compressed so a demo shows some)")
    p.add_argument("--refuels-per-min", type=float, default=20, help="refuels starting a minute across the fleet")
    p.add_argument("--notifiers", type=int, default=3, help="services reading fuel-alerts, each with its own reader")
    p.add_argument("--poll-ms", type=float, default=1, help="notifier wait between polls when nothing is waiting")
    p.add_argument("--compare-s", type=float, default=60,
                   help="also count what a drain alert this much later would still have caught")
    p.add_argument("--peak", type=float, default=1, help="drain multiplier during the peak (night halts)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
