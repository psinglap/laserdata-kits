"""The fuel rules, fed by the fuel ping lane.

Reads every partition of fuel-pings directly, on --readers connections of its own, tracking its own position and
storing it on the broker at most once a second (no consumer group: a group reader visits one partition per poll,
so with 16 partitions a ping waits for its partition's turn and a busy host pushes the tail past the budget).

Each record is one ping from one truck's GPS device, as the ingestion layer formats it: fuel level in litres from
the tank sensor, ignition, speed, position and the device's ping sequence. The fuel level is the resulting value,
never a delta, so a replayed or repeated ping is recognised by its sequence and changes nothing.

Two rules (stand-ins for yours; put your own in Truck.apply()), checked against ignition, speed and a time
window, on a smoothed signal:
  * DRAIN: while the truck is parked (ignition off, speed 0), the smoothed fuel level falls DRAIN_L litres below
    the highest level seen at this stop. One alert per stop.
  * REFUEL: the smoothed level rises REFUEL_L litres above the lowest level seen at this stop. Recorded and sent
    as an event, never as an alarm, so a refuel is never mistaken for a theft.
Smoothing is the median of the last 3 readings, so a single spike from vibration, sloshing or the electrical
supply never raises an alert. Per-truck state is rebuilt from that truck's next pings after a restart.
Alerts go to fuel-alerts, which every notifier reads (trucks.py plays the owner app, the fleet dashboard and SMS),
in small batches from their own task, never from inside the read loop.

Prints ping-to-rules p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the alerts.
"""
import argparse
import asyncio
import gc
import statistics
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (ALERTS, DEFAULT_URL, DRAIN, DRAIN_L, PARTITIONS, PINGS, REFUEL, REFUEL_L, SMOOTH, STREAM, Stalls,
                    Window, ensure_topics, load, msg)


class Truck:
    __slots__ = ("seq", "win", "parked", "high", "low", "drained", "refuelled")

    def __init__(self):
        self.seq = 0
        self.win = deque(maxlen=SMOOTH)
        self.parked = False
        self.high = self.low = None         # highest and lowest smoothed level at this stop
        self.drained = self.refuelled = False

    def apply(self, d):
        """Returns (kind, litres) or None for one ping, in this truck's order."""
        if d["ign"] or d["spd"] > 0:
            self.parked = False             # moving or ignition on: the stop is over
            self.win.clear()
            return None
        if not self.parked:                 # the first ping of a new stop: fresh window and marks
            self.parked, self.drained, self.refuelled = True, False, False
            self.high = self.low = None
            self.win.clear()
        self.win.append(d["fuel"])
        if len(self.win) < SMOOTH:
            return None
        level = statistics.median(self.win)
        if self.high is None:
            self.high = self.low = level
        self.high, self.low = max(self.high, level), min(self.low, level)
        return self.check(level)

    def check(self, level):
        if not self.refuelled and level - self.low >= REFUEL_L:
            self.refuelled = True
            self.high = level               # a drain after a refuel is measured from the refilled level
            return REFUEL, round(level - self.low, 1)
        if not self.drained and self.high - level >= DRAIN_L:
            self.drained = True
            return DRAIN, round(self.high - level, 1)
        return None


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), PINGS, ALERTS)
    out = await (await own_client()).producer(STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS])
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    trucks = defaultdict(Truck)             # device -> its fuel state at the current stop
    pending = []                            # alerts waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays pings from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_ping(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
            b = m.timestamp() / 1e3                 # broker append time, ms: where the time went
            stats["to_broker"] = max(stats["to_broker"], b - d["ns"] / 1e6)
            stats["from_broker"] = max(stats["from_broker"], now / 1e6 - b)
        else:                               # still replaying: track the oldest ping seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        t = trucks[d["d"]]
        if d["seq"] <= t.seq:
            stats["dropped"] += 1           # older or repeated ping: already applied
            return
        t.seq = d["seq"]
        stats["pings"] += 1
        stats["parked"] += not d["ign"]
        r = t.apply(d)
        if r is None:
            return
        kind, litres = r
        stats[kind] += 1
        if age > a.replay_alert_s * 1000:
            return                          # replayed from long ago: state rebuilt, nobody needs this alert now
        pending.append(SendMessage(msg(d=d["d"], kind=kind, litres=litres, ep=d["seq"], src_ns=d["ns"],
                                       fuel=d["fuel"], lat=d["lat"], lon=d["lon"])))

    async def read(pids):
        # Each partition in turn. Resume from the broker's stored offset once (from the start of the log the
        # first time), then read on from our own position, storing the offset at most once a second: an offset
        # write on every poll stalls the lane. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-p{p}") for p in pids}
        nxt, committed = {}, {}
        while True:
            more = False
            for p in pids:
                commit = time.monotonic() - committed.get(p, 0) >= a.commit_s
                ms = await c.poll_messages(STREAM, PINGS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=commit,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.Next())
                if ms:
                    if commit:
                        committed[p] = time.monotonic()
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_ping(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_alerts():
        # Alerts are small and come in bursts; one batch every 2 ms keeps sends out of the read loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of pings arrived inside the budget; partitions interleave,
                # so one fresh ping alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier pings this second, "
                      f"state rebuilt for {len(trucks):,} trucks", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} pings/s  ping-to-rules p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  parked {int(stats['parked']):,}  "
                  f"alerts sent {int(stats['sent']):,} (drain {int(stats[DRAIN]):,}, refuel {int(stats[REFUEL]):,})  "
                  f"{stalls.read()}  slowest ping->broker {stats['to_broker']:.0f} ms, "
                  f"broker->rules {stats['from_broker']:.0f} ms", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[PINGS]))
    await asyncio.gather(send_alerts(), report(), stalls.watch(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="fuel-rules")
    p.add_argument("--budget-ms", type=int, default=50, help="ping-to-rules p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the fuel-pings partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--commit-s", type=float, default=1, help="store each partition's offset at most this often")
    p.add_argument("--replay-alert-s", type=float, default=30,
                   help="after a restart, still send alerts for pings younger than this")
    asyncio.run(main(p.parse_args()))
