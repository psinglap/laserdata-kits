"""A fleet of connected scooters, the ingest step behind the MQTT broker, and the notifiers that read every alert.

Plays --scooters scooters in one process. Each scooter's Cloud Connect module sends its events (a ride starting
or ending, charging starting or complete, a state-of-charge update, movement while parked, a crash), one every
--event-s seconds per scooter on average, each with the scooter's own clock (vt), its event sequence and its whole
current state. The ingest step routes each event by its age on the scooter's clock (common.route()):
  * fresh events, and every crash, go on rider-events, the lane the alert service reads;
  * events older than common.FRESH_S go on reconnect-backlog, which ride history reads at its own pace.

Two things from the field are played on purpose:
  * a lost acknowledgement: --resend-share of events are sent a second time, with the same sequence, as a module
    that saves data until the cloud acknowledges it would;
  * a reconnect storm: --storm-share of the fleet is offline from the start (a network outage) and comes back at
    --storm-at, each scooter flushing --offline-min minutes of buffered events over --flush-s seconds.

--notifiers readers play the services that act on rider-alerts (app push, the SMS gateway for crash alerts,
the support console), each on its own connection, reading the alerts partition directly from its own position.
The app push reader checks every alert against the truth this process holds: each fresh alert-worthy event
reached the app once, on time; none was pushed twice; no stale event buzzed a rider's phone as news.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ALERT_KINDS, ALERTS, ALWAYS_SEND, BACKLOG, CHARGE_DONE, CHARGE_START, CRASH, DEFAULT_URL,
                    FRESH_S, LANE, MOVED, PARTITIONS, STREAM, Stalls, Window, ensure_topics, load, msg, part, route,
                    scooter)

NOTIFIERS = ["app-push", "sms-gateway", "support-console", "ride-timeline", "partner-webhooks"]
EDGE_S = 5                          # events within 5 s of FRESH_S may land either side; they are not judged


class Scooter:
    __slots__ = ("s", "seq", "ch", "soc", "pk", "lat", "lon", "nxt", "online")

    def __init__(self, s):
        self.s, self.seq = s, 0
        self.ch, self.pk = 0, 1
        self.soc = random.randint(20, 95)
        self.lat, self.lon = 12.97 + random.uniform(-0.3, 0.3), 77.59 + random.uniform(-0.3, 0.3)
        self.nxt, self.online = 0.0, True

    def step(self, crash_rate, moved_rate):
        """The scooter's next event and its new state (illustrative mix; yours come from the fleet)."""
        r = random.random()
        if not self.pk:                                     # riding
            if r < crash_rate:
                k = CRASH
            elif r < 0.35:
                k, self.pk = "ride_end", 1
            else:
                k = "soc"
            self.soc = max(5, self.soc - random.randint(0, 3))
            self.lat += random.uniform(-0.01, 0.01)
            self.lon += random.uniform(-0.01, 0.01)
        elif self.ch:                                       # parked and charging
            self.soc = min(100, self.soc + random.randint(2, 8))
            if r < 0.3 or self.soc == 100:
                k, self.ch, self.soc = CHARGE_DONE, 0, 100 if self.soc > 95 else self.soc
            else:
                k = "soc"
        else:                                               # parked
            if r < moved_rate:
                k = MOVED
            elif r < 0.45:
                k, self.pk = "ride_start", 0
            elif r < 0.75 and self.soc < 90:
                k, self.ch = CHARGE_START, 1
            else:
                k = "soc"
        self.seq += 1
        return dict(s=self.s, seq=self.seq, k=k, ch=self.ch, soc=self.soc, pk=self.pk, lat=round(self.lat, 5),
                    lon=round(self.lon, 5))


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LANE, BACKLOG, ALERTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    lane = await (await own_client()).producer(STREAM, LANE, topic_partitions_count=PARTITIONS[LANE], mode=cfg)
    backlog = await (await own_client()).producer(STREAM, BACKLOG, topic_partitions_count=PARTITIONS[BACKLOG],
                                                  mode=DirectProducerConfig(batch_length=5000,
                                                                            linger_time=timedelta(milliseconds=2)))
    start_ns = time.time_ns()
    fleet = [Scooter(scooter(i)) for i in range(a.scooters)]
    pid = [part(x.s, PARTITIONS[LANE]) for x in fleet]
    storm = set(random.sample(range(a.scooters), int(a.scooters * a.storm_share))) if a.storm_at else set()
    now = time.monotonic()
    due = []
    for i, x in enumerate(fleet):
        if i in storm:
            x.online = False                # out of coverage since before the run began; buffering on the scooter
        else:
            x.nxt = now + random.uniform(0, a.event_s)
            due.append((x.nxt, i))
    heapq.heapify(due)
    crash_rate, moved_rate = a.crash_rate, a.moved_rate
    expected = {}                           # (scooter, seq) -> (kind, routed at ns): alerts the app must get
    order = deque()                         # (routed at ns, key), to find alerts that never arrived
    seen = set()                            # (scooter, seq) the app already pushed
    lat, stats, total, stalls = Window(), defaultdict(float), defaultdict(int), Stalls()

    def expect(d, now_ns):
        """Record what the app must receive for an event the ingest step put on the lane."""
        if d["k"] not in ALERT_KINDS:
            return
        age = (now_ns - d["vt"]) / 1e9
        if d["k"] in ALWAYS_SEND or age <= FRESH_S - EDGE_S:
            expected[(d["s"], d["seq"])] = (d["k"], now_ns)
            order.append((now_ns, (d["s"], d["seq"])))
            stats["expected"] += 1

    def on_alert(m):
        d = load(m.payload())
        if d["src_ns"] <= start_ns:
            return                          # from an earlier run
        now_ns = time.time_ns()
        key = (d["s"], d["seq"])
        if key in seen:
            stats["twice"] += 1             # the rider's phone would buzz twice for one event
            return
        seen.add(key)
        if (d["src_ns"] - d["vt"]) > FRESH_S * 1e9 and d["kind"] not in ALWAYS_SEND:
            stats["stale"] += 1             # history pushed as news
        if d.get("delayed"):
            stats["late_crash"] += 1        # a crash buffered offline: still sent, marked as delayed
        if key in expected:
            del expected[key]
            ms = (now_ns - d["src_ns"]) / 1e6
            lat.add(ms)
            stats["at_app"] += 1
            stats["on_time"] += ms < a.on_time_ms

    async def notify(name):
        # One connection per notifier, reading the alerts partition directly from its own position (no consumer
        # group, nothing stored on the broker per poll); a full batch means more is waiting, so no pause.
        c = await own_client()
        who, nxt = Consumer.Single(f"{name}-{os.getpid()}"), None
        while True:
            ms = await c.poll_messages(STREAM, ALERTS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                if name == "app-push":
                    for m in ms:
                        on_alert(m)
            if len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send(batches, producer):
        # One batch per partition, all sent at once: per-scooter order is kept, one round trip per tick.
        if batches:
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in batches.items()))

    gc.freeze()                             # keep start-up objects out of the collector's way
    resend = []                             # events whose acknowledgement was lost: sent again next tick

    async def live_loop():
        nonlocal resend
        t0 = tick = last = time.monotonic()
        extra = 0.0
        while True:
            now = time.monotonic()
            x_peak = a.peak if a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1
            if x_peak > 1:                  # the peak: bring extra events forward, at (peak - 1) x the base rate
                extra += (x_peak - 1) * a.scooters / a.event_s * (now - last)
                while extra >= 1:
                    extra -= 1
                    j = random.randrange(a.scooters)
                    if fleet[j].online:
                        fleet[j].nxt = now
                        heapq.heappush(due, (now, j))
            last = now
            batches = defaultdict(list)
            now_ns = time.time_ns()
            for d in resend:
                batches[part(d["s"], PARTITIONS[LANE])].append(SendMessage(msg(**d)))
                stats["resends"] += 1
            resend = []
            while due and due[0][0] <= now:
                t, i = heapq.heappop(due)
                x = fleet[i]
                if t != x.nxt or not x.online:
                    continue                # superseded by an event the peak brought forward
                d = x.step(crash_rate, moved_rate)
                d["vt"] = now_ns            # live: the scooter's clock and the cloud's agree to the network delay
                expect(d, now_ns)
                batches[pid[i]].append(SendMessage(msg(**d)))
                if random.random() < a.resend_share:
                    resend.append(d)
                stats["events"] += 1
                x.nxt = t + random.expovariate(1 / a.event_s)      # from its due time, so a late tick never bunches
                heapq.heappush(due, (x.nxt, i))
            await send(batches, lane)
            stats["peak"] = x_peak > 1
            tick += 0.01
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def reconnect_storm():
        """The outage ends: every scooter in it reconnects within --flush-s seconds and its Cloud Connect module
        flushes what it buffered, oldest first, then resends its last event (the acknowledgement was lost when the
        network dropped). The ingest step routes each event by its age on the scooter's clock."""
        if not storm:
            return
        await asyncio.sleep(a.storm_at)
        stats["storm"] = 1
        back_at = time.time_ns()
        offline_ns = int(a.offline_min * 60e9)
        n_each = max(1, int(a.offline_min * 60 / a.event_s))
        queue = list(storm)
        random.shuffle(queue)
        per_tick = max(1, int(len(queue) / (a.flush_s * 100)))
        while queue:
            now_ns = time.time_ns()
            lane_b, back_b = defaultdict(list), defaultdict(list)
            for i in queue[:per_tick]:
                x = fleet[i]
                for j in range(n_each):
                    d = x.step(crash_rate, moved_rate)
                    d["vt"] = back_at - offline_ns + int((j + 1) * offline_ns / (n_each + 1))
                    events = [d] + ([dict(d)] if j == n_each - 1 else [])     # the last one twice: lost ack
                    for e in events:
                        if route(e["k"], e["vt"], now_ns) == LANE:
                            if e is d:
                                expect(e, now_ns)
                            lane_b[pid[i]].append(SendMessage(msg(**e)))
                            stats["flushed_live"] += 1
                        else:
                            back_b[part(x.s, PARTITIONS[BACKLOG])].append(SendMessage(msg(**e)))
                            stats["backlog"] += 1
                            total["backlog"] += 1
            await asyncio.gather(send(lane_b, lane), send(back_b, backlog))
            for i in queue[:per_tick]:      # back online: live events follow the flush, in sequence order
                x = fleet[i]
                x.online, x.nxt = True, time.monotonic() + random.uniform(0, a.event_s)
                heapq.heappush(due, (x.nxt, i))
                total["back"] += 1
            del queue[:per_tick]
            await asyncio.sleep(0.01)
        stats["storm"] = 0
        print(f"[scooters] reconnect storm done: {len(storm):,} scooters back online, {total['backlog']:,} buffered "
              f"events to '{BACKLOG}' in total", flush=True)

    async def report():
        print(f"[scooters] {a.scooters:,} scooters, an event every {a.event_s:g} s each, {a.resend_share:.1%} resent "
              f"(lost ack), {len(storm):,} offline for {a.offline_min:g} min reconnecting at {a.storm_at:g} s, "
              f"{a.notifiers} notifiers, fresh window {FRESH_S} s", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            cutoff = time.time_ns() - int(a.missing_s * 1e9)
            while order and order[0][0] < cutoff:
                _, key = order.popleft()
                if expected.pop(key, None) is not None:
                    stats["missing"] += 1   # never reached the app
            total["missing"] += int(stats["missing"])
            phase = "reconnect" if stats["storm"] else "peak" if stats["peak"] else "steady"
            print(f"[scooters] {int(stats['events'] + stats['flushed_live']):>6,} events/s ({phase})  "
                  f"online {a.scooters - len(storm) + total['back']:,}  alerts expected {int(stats['expected']):,}  "
                  f"at the app {int(stats['at_app']):,}, on time (<{a.on_time_ms:g} ms) {int(stats['on_time']):,}  "
                  f"missing {int(stats['missing']):,}  pushed twice {int(stats['twice']):,}  stale pushed "
                  f"{int(stats['stale']):,}  late crash sent {int(stats['late_crash']):,}  resent {int(stats['resends']):,}  "
                  f"to backlog {int(stats['backlog']):,} (total {total['backlog']:,})  "
                  f"event-to-app n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  {stalls.read()}", flush=True)
            for k in ("events", "flushed_live", "expected", "at_app", "on_time", "missing", "twice", "stale",
                      "late_crash", "resends", "backlog"):
                stats[k] = 0

    async with lane, backlog:
        await asyncio.gather(live_loop(), reconnect_storm(), report(), stalls.watch(),
                             *(notify(n) for n in NOTIFIERS[: a.notifiers]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--scooters", type=int, default=500_000, help="connected scooters")
    p.add_argument("--event-s", type=float, default=100, help="mean seconds between rider-facing events per scooter")
    p.add_argument("--crash-rate", type=float, default=0.0005, help="chance a riding event is a crash")
    p.add_argument("--moved-rate", type=float, default=0.01, help="chance a parked event is movement while parked")
    p.add_argument("--resend-share", type=float, default=0.01, help="share of events sent twice (lost ack)")
    p.add_argument("--storm-at", type=float, default=60, help="seconds until the outage ends (0 = no storm)")
    p.add_argument("--storm-share", type=float, default=0.05, help="share of the fleet in the outage")
    p.add_argument("--offline-min", type=float, default=10, help="minutes the outage lasted")
    p.add_argument("--flush-s", type=float, default=10, help="seconds over which the scooters reconnect")
    p.add_argument("--notifiers", type=int, default=3, help="services reading rider-alerts, each with its own reader")
    p.add_argument("--poll-ms", type=float, default=1, help="notifier wait between polls when nothing is waiting")
    p.add_argument("--on-time-ms", type=float, default=1000, help="an alert at the app within this is on time")
    p.add_argument("--missing-s", type=float, default=5, help="an expected alert not at the app after this is missing")
    p.add_argument("--peak", type=float, default=1, help="event-rate multiplier during the peak (evening commute)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
