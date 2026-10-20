"""The source of truth: a fleet of connected vehicles, each sending a report every --every seconds.

Every awake vehicle reports on its own five-second schedule, keyed by vehicle, with a per-vehicle sequence
number and the resulting value of every signal the app shows: state of charge, range, charging state and
power, locks, closures, cabin temperature, gear, odometer and position. Vehicles park, drive, plug in and
charge, lock and unlock, so the state changes the way owners see it change.

vehicles.py also reads every app open the latest-state service served (the app-reads topic) and checks it
against the truth: did the app show the vehicle's latest report, or one the vehicle had already replaced?
And if replaced, was the lock or charging state on screen wrong? That is what a slow lane costs an owner.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, PARTITIONS, READS, STATE, STREAM, Stalls, ensure_topics, load, msg, part, vehicle

TICK = 0.02   # seconds; each vehicle still reports on its own schedule, stamped when sent
PACK_KWH = 135.0                      # illustrative pack size, for state of charge and range


class Vehicle:
    __slots__ = ("id", "seq", "mode", "soc", "target", "kw", "locked", "closures", "cabin", "odo", "lat", "lon",
                 "until", "sent")

    def __init__(self, i, r):
        self.id = vehicle(i)
        self.seq, self.mode = 0, "parked"
        self.soc, self.target, self.kw = r.uniform(25, 90), 80.0, 0.0
        self.locked, self.closures = True, 0
        self.cabin, self.odo = r.uniform(12, 24), r.uniform(500, 40000)
        self.lat, self.lon = r.uniform(33.0, 47.0), r.uniform(-122.0, -75.0)
        self.until = 0.0                      # when the current drive or unlocked spell ends
        self.sent = deque(maxlen=8)           # (seq, sent_ns, locked, charge) of recent reports, for the app check


def charge_state(v):
    return "charging" if v.mode == "charging" else ("complete" if v.kw == 0 and v.soc >= v.target - 0.5 and
                                                    v.mode == "plugged" else "idle")


def step(v, now, every, r):
    """Advance one vehicle by one report interval (illustrative behaviour, sized so changes are frequent)."""
    if v.mode == "driving":
        v.soc = max(5.0, v.soc - 0.03 * every)                 # ~2 kWh per mile-ish, simplified
        v.odo += 0.012 * every * r.uniform(0.6, 1.2)
        v.lat += r.uniform(-0.0004, 0.0004)
        v.lon += r.uniform(-0.0004, 0.0004)
        if now >= v.until:
            v.mode, v.locked = "parked", False
            v.until = now + r.uniform(5, 60)                   # unlocked for a little while after parking
    elif v.mode == "charging":
        v.soc = min(v.target, v.soc + v.kw * every / 3600 / PACK_KWH * 100)
        if v.soc >= v.target:
            v.mode, v.kw = "plugged", 0.0                      # charge complete, still plugged in
    else:                                                       # parked or plugged
        if not v.locked and now >= v.until:
            v.locked, v.closures = True, 0                     # walk-away lock
        elif v.locked and r.random() < 0.004:
            v.locked, v.until = False, now + r.uniform(5, 60)  # owner unlocks
            v.closures = r.choice((0, 0, 1, 2))                # a door, the frunk or the liftgate open
        elif v.mode == "parked" and r.random() < 0.003:
            v.mode, v.locked, v.until = "driving", False, now + r.uniform(60, 600)
        elif v.mode == "parked" and v.soc < 70 and r.random() < 0.004:
            v.mode, v.kw = "charging", r.choice((11.0, 11.0, 48.0, 150.0))   # home, wall charger, fast charger
            v.target = r.choice((70.0, 80.0, 90.0))
        elif v.mode == "plugged" and r.random() < 0.003:
            v.mode = "parked"                                  # unplugged
    v.cabin += (21.0 - v.cabin) * 0.02 if v.mode == "driving" else r.uniform(-0.05, 0.05)


def report(v, ns):
    return msg(v=v.id, seq=v.seq, soc=round(v.soc, 1), range_mi=round(v.soc * 3.4), charge=charge_state(v),
               charge_kw=v.kw if v.mode == "charging" else 0.0, locked=v.locked, closures=v.closures,
               cabin_c=round(v.cabin, 1), gear="D" if v.mode == "driving" else "P", odo_mi=round(v.odo, 1),
               lat=round(v.lat, 5), lon=round(v.lon, 5), ns=ns)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), STATE, READS)
    cfg = DirectProducerConfig(batch_length=5000, linger_time=timedelta(microseconds=500))
    out = await (await own_client()).producer(STREAM, STATE, topic_partitions_count=PARTITIONS[STATE], mode=cfg)
    reads = await (await own_client()).consumer_group(
        "vehicles-app-check", STREAM, READS, polling_strategy=PollingStrategy.Next(), batch_length=5000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)),     # offsets stored once a second, not per poll
        poll_interval=timedelta(milliseconds=5))                    # a check, not on the lane's clock

    r = random.Random(7)
    pool = [Vehicle(i, r) for i in range(int(a.vehicles * max(1.0, a.peak)))]
    by_id = {v.id: v for v in pool}
    slots = max(1, round(a.every / TICK))
    ring = [[] for _ in range(slots)]                        # each vehicle reports in its own slot, staggered
    for i in range(len(pool)):
        ring[i % slots].append(i)
    stats, stalls = defaultdict(int), Stalls()
    gc.freeze()                                              # keep the fleet out of the collector's way

    async def send_by_partition(items):
        # One batch per partition, all sent at once: per-vehicle order kept, about one round trip per tick.
        batches = defaultdict(list)
        for key, m in items:
            batches[part(key, PARTITIONS[STATE])].append(m)
        await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))

    async def drive():
        async with out:
            t0 = due = time.monotonic()
            tick = 0
            while True:
                now, now_ns = time.monotonic(), time.time_ns()
                peak = a.peak if a.peak_at and a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1.0
                awake = int(a.vehicles * peak)
                items = []
                for i in ring[tick % slots]:
                    if i >= awake:
                        continue                             # asleep: a parked vehicle that is not reporting
                    v = pool[i]
                    before = (v.locked, charge_state(v))
                    step(v, now, a.every, r)
                    v.seq += 1
                    after = (v.locked, charge_state(v))
                    stats["changes"] += before != after
                    v.sent.append((v.seq, now_ns) + after)
                    items.append((v.id, SendMessage(report(v, now_ns))))
                stats["reports"] += len(items)
                stats["awake"] = awake
                await send_by_partition(items)
                tick += 1
                due += TICK
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def on_read(m):
        d = load(m.payload())
        v = by_id.get(d["v"])
        if v is None:
            return
        prior = [s for s in v.sent if s[1] <= d["ns"]]       # reports the vehicle had sent when the app opened
        if not prior or d["seq"] < v.sent[0][0]:
            return                                           # older than the check's memory: not judged
        latest = max(prior)
        behind = latest[0] - d["seq"]
        stats["checked"] += 1
        stats["changed_read"] += d["chg"]
        if behind <= 0:
            stats["latest"] += 1
            return
        stats["behind1" if behind == 1 else "behind2"] += 1
        if (latest[2], latest[3]) != (d["locked"], d["charge"]):
            stats["wrong"] += 1                              # the owner saw the wrong lock or charging state

    async def report_line():
        while True:
            await asyncio.sleep(1)
            c = stats["checked"]
            pct = lambda k: 100 * stats[k] / c if c else 0.0
            print(f"[vehicles] {stats['reports']:>7,} reports/s  awake {stats['awake']:,}  lock or charge changes "
                  f"{stats['changes']:,}  app reads checked {c:,} (right after a change {stats['changed_read']:,})  "
                  f"on the latest report {pct('latest'):6.2f}%  one behind {pct('behind1'):5.2f}%  "
                  f"two or more behind {pct('behind2'):5.3f}%  wrong lock or charge state shown {stats['wrong']:,}  "
                  f"{stalls.read()}", flush=True)
            for k in ("reports", "changes", "checked", "changed_read", "latest", "behind1", "behind2", "wrong"):
                stats[k] = 0

    await asyncio.gather(drive(), reads.consume_messages(on_read, asyncio.Event()), report_line(), stalls.watch())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--vehicles", type=int, default=150000, help="connected vehicles reporting")
    ap.add_argument("--every", type=float, default=5.0, help="seconds between reports per vehicle")
    ap.add_argument("--peak", type=float, default=1.0, help="multiplier for vehicles awake during the peak")
    ap.add_argument("--peak-at", type=float, default=0, help="seconds after start the peak begins")
    ap.add_argument("--peak-for", type=float, default=120, help="seconds the peak lasts")
    asyncio.run(main(ap.parse_args()))
