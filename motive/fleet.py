"""A fleet of trailers and reefers with Asset Gateways and Door and Environmental Sensors, and the notifiers that
read every alert.

Plays --assets assets in one process. Each asset's gateway publishes to cargo-sensor-events, keyed by asset:
  * a door change the moment a cargo door opens or closes;
  * a reading every --reading-s seconds (Motive documents one-minute environmental readings).
Every record carries the asset's whole current state: door open or closed and since when, cargo temperature,
set point and position, with the asset's event sequence. Doors open at stops, mostly inside an approved
geofence (yard, customer site) and sometimes outside one; an open door lets the cargo warm up, and a share of
reefer units drift out of range on their own. Door durations are illustrative: most stops 20-90 s, some
loading at a dock for 2-15 min (set them to yours in door_minutes()).

--notifiers readers play the services that act on cargo-alerts (driver app push, fleet dashboard, webhooks),
each on its own connection and consumer group, offsets stored once a second.

The business number: of the alerts for doors opened outside an approved geofence, how many reached the driver
app while that door was still open, so someone can still act on it. Prints it every second, with
event-to-notifier p50/p99 (the whole loop: gateway event, alert rules, alert, notifier).
"""
import argparse
import asyncio
import gc
import heapq
import math
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ALERTS, DEFAULT_URL, DOOR_OUTSIDE, PARTITIONS, READINGS, STREAM, TEMP_HIGH, Window, asset,
                    ensure_topics, load, msg, part, set_point, sites)

NOTIFIERS = ["driver-app", "fleet-dashboard", "webhooks", "ops-console", "integrations", "reports"]


def door_minutes():
    """How long a door stays open (illustrative; set it to yours): most stops are short deliveries,
    some are loading at a dock."""
    if random.random() < 0.7:
        return random.uniform(20, 90) / 60
    return random.uniform(2, 15)


class Asset:
    __slots__ = ("a", "seq", "door", "door_ns", "temp", "sp", "t", "pos", "fault", "ep")

    def __init__(self, a):
        self.a, self.seq, self.door, self.door_ns = a, 0, 0, 0
        self.sp = set_point(a)
        self.temp = self.sp + random.uniform(-1, 1)
        self.t = time.monotonic()
        self.pos = self.transit()
        self.fault = 0.0                    # monotonic time until which the reefer unit is failing
        self.ep = 0                         # sequence of the door-open event of the current opening

    def transit(self):
        lat, lon = random.choice(sites(self.a))
        d = random.uniform(5_000, 50_000) / 111_000        # on the road between sites
        return (lat + d * random.choice((-1, 1)), lon + d * random.choice((-1, 1)))

    def warm(self, now):
        """Cargo temperature since the last event: an open door or a failing unit warms it, a working unit
        pulls it back to the set point."""
        dt, self.t = now - self.t, now
        if self.door or now < self.fault:
            self.temp += (0.03 if self.door else 0.01) * dt
        else:
            self.temp += (self.sp - self.temp) * min(1.0, 0.01 * dt)
        return round(self.temp + random.uniform(-0.2, 0.2), 1)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), READINGS, ALERTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, READINGS, topic_partitions_count=PARTITIONS[READINGS],
                                                   mode=cfg)
    start_ns = time.time_ns()
    assets = [Asset(asset(i)) for i in range(a.assets)]
    pid = [part(x.a, PARTITIONS[READINGS]) for x in assets]
    door_gap = a.door_every_min * 60        # mean seconds between door openings per asset
    now = time.monotonic()
    due = [(now + random.uniform(0, a.reading_s), i, "reading") for i in range(a.assets)]
    due += [(now + random.expovariate(1 / door_gap), i, "open") for i in range(a.assets)]
    heapq.heapify(due)
    opened = {}                             # (asset, open seq) -> (door open for s, opened at ns), outside only
    lat, stats = Window(), defaultdict(float)
    seen = set()                            # (asset, kind, ep) the driver app already has

    def reader(name):
        async def on_alert(m):
            d = load(m.payload())
            if d["src_ns"] > start_ns:
                lat.add((time.time_ns() - d["src_ns"]) / 1e6)   # live alerts only, not ones replayed at start-up
            if name != "driver-app" or (d["a"], d["kind"], d["ep"]) in seen:
                return                      # repeated alert (at-least-once): already counted
            seen.add((d["a"], d["kind"], d["ep"]))
            stats[d["kind"]] += 1
            if d["kind"] == DOOR_OUTSIDE:
                held, f_ns = opened.pop((d["a"], d["ep"]), (None, 0))
                if held is None:
                    return
                stats["door_alerts"] += 1
                x = assets_by[d["a"]]
                if x.door and x.ep == d["ep"]:
                    stats["while_open"] += 1        # the driver can still close it or explain it
                if held > a.compare_s:
                    stats["open_at_compare"] += 1   # a lane this slow would still have found it open
        return on_alert

    assets_by = {x.a: x for x in assets}
    readers = []
    for name in NOTIFIERS[: a.notifiers]:
        readers.append((await (await own_client()).consumer_group(
            name, STREAM, ALERTS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
            auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500)),
            reader(name)))
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def gateway_loop():
        t0 = tick = time.monotonic()
        async with producer:
            while True:
                now = time.monotonic()
                x_peak = a.peak if a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1
                batches = defaultdict(list)
                while due and due[0][0] <= now:
                    _, i, kind = heapq.heappop(due)
                    s = assets[i]
                    temp = s.warm(now)
                    s.seq += 1
                    if kind == "reading":
                        if s.door == 0 and now >= s.fault and random.random() < a.fault_rate:
                            s.fault = now + random.uniform(300, 900)     # the reefer unit starts to fail
                        heapq.heappush(due, (now + a.reading_s, i, "reading"))
                        stats["readings"] += 1
                    elif kind == "open":
                        site = random.choice(sites(s.a))
                        outside = random.random() < a.outside_share     # a stop outside every approved geofence
                        if outside:
                            d = random.uniform(2_000, 20_000) / 111_000
                            s.pos = (site[0] + d, site[1] - d)
                        else:
                            j = random.uniform(-80, 80) / 111_000
                            s.pos = (site[0] + j, site[1] + j)
                        held = door_minutes() * 60
                        s.door, s.door_ns, s.ep = 1, time.time_ns(), s.seq
                        if outside:
                            opened[(s.a, s.seq)] = (held, s.door_ns)
                            stats["outside_opens"] += 1
                        heapq.heappush(due, (now + held, i, "close"))
                        stats["doors"] += 1
                    else:                    # close: the door shuts and the asset drives on
                        s.door, s.door_ns = 0, time.time_ns()
                        s.pos = s.transit()
                        heapq.heappush(due, (now + random.expovariate(x_peak / door_gap), i, "open"))
                        stats["doors"] += 1
                    batches[pid[i]].append(SendMessage(msg(
                        a=s.a, seq=s.seq, k="reading" if kind == "reading" else "door", door=s.door,
                        door_ns=s.door_ns, temp=temp, sp=s.sp, lat=round(s.pos[0], 6), lon=round(s.pos[1], 6))))
                # One batch per partition, all sent at once: per-asset order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["peak"] = x_peak > 1
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[fleet] {a.assets:,} assets, a reading every {a.reading_s:g} s, a door opening every "
              f"{a.door_every_min:g} min per asset, {a.outside_share:.0%} of stops outside an approved geofence, "
              f"{a.notifiers} notifiers", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            print(f"[fleet] {int(stats['readings'] + stats['doors']):>6,} events/s ({phase})  door changes "
                  f"{int(stats['doors']):,}  doors opened outside a geofence {int(stats['outside_opens']):,}  "
                  f"door alerts at the driver app {int(stats['door_alerts']):,}, while the door was still open "
                  f"{int(stats['while_open']):,} (at {a.compare_s:g} s it would be {int(stats['open_at_compare']):,})  "
                  f"temperature alerts {int(stats[TEMP_HIGH]):,}  "
                  f"event-to-notifiers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("readings", "doors", "outside_opens", "door_alerts", "while_open", "open_at_compare",
                      DOOR_OUTSIDE, TEMP_HIGH):
                stats[k] = 0

    await asyncio.gather(gateway_loop(), report(), *(c.consume_messages(cb, asyncio.Event()) for c, cb in readers))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--assets", type=int, default=300_000, help="assets with a gateway and cargo sensors")
    p.add_argument("--reading-s", type=float, default=60, help="seconds between environmental readings per asset")
    p.add_argument("--door-every-min", type=float, default=120, help="mean minutes between door openings per asset")
    p.add_argument("--outside-share", type=float, default=0.05, help="share of door openings outside a geofence")
    p.add_argument("--fault-rate", type=float, default=0.0005, help="chance per reading that a reefer unit starts to fail")
    p.add_argument("--notifiers", type=int, default=3, help="services reading cargo-alerts, each with its own reader")
    p.add_argument("--compare-s", type=float, default=60,
                   help="also count door alerts a lane this slow would still have delivered while the door was open")
    p.add_argument("--peak", type=float, default=1, help="door-opening multiplier during the peak (morning dispatch)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
