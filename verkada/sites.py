"""Alarm sites with camera partitions, and the responders that read every raised alarm.

Plays --partitions camera partitions (two per site) in one process. Every event for a partition goes to
camera-partition-events, keyed by alarm partition, with the partition's event sequence:
  * detect: a camera trigger (person detection, line crossing, loitering), from the partition's cameras;
  * disarm: an access-granted event at a door configured to disarm it (badge, Face Unlock, Mobile NFC, entry code);
  * arm: two badges within 5 seconds, a schedule, Command or the keypad;
  * panic: a digital panic button pressed in Command or the app.
At start, every partition's current state is sent once (sequence 1) so the alarm state service knows which are armed.

What happens (rates are flags; the shapes are illustrative, set them to yours):
  * Disarmed partitions see people all day: background detections, which must reach the alarm state service in
    order but change nothing.
  * Arrivals: someone walks into an armed partition, a camera sees them (one to three detections), and they
    disarm at a door before the entry delay runs out. Most disarm well inside the delay; --tail-share of them in
    its last 3 seconds (a long walk from the car park, a second door, a slow badge).
  * Intrusions: someone is seen in an armed partition and nobody disarms: an alarm is due when the delay runs out.
  * Panic presses: a silent alarm, due at once.

--responders readers play the services that act on alarms-raised (the monitoring agents' queue, notifications,
the response workflows), each on its own connection, reading the alarms partition directly from its own position.

The business numbers, every second: of the arrivals that disarmed in time, how many still got an alarm raised
(a false alarm sent to a monitoring agent, caused only by the disarm reaching the alarm state late), and how many
would have with a lane --compare-s slower; and whether every intrusion and panic alarm reached the responders.
Prints due-to-responders p50/p99: from the moment an alarm is due (the entry delay runs out, or the panic press)
to all responders having it.
"""
import argparse
import asyncio
import gc
import heapq
import itertools
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ARM, DEFAULT_URL, DETECT, DISARM, DISARM_BY, EVENTS, PANIC, PARTITIONS, RAISED, STREAM,
                    TRIGGERS, Stalls, Window, ensure_topics, load, msg, part, partition)

RESPONDERS = ["monitoring-agents", "notifications", "response-workflows", "integrations", "audit-log"]


class AP:
    __slots__ = ("k", "seq", "armed", "busy", "pid", "cam")

    def __init__(self, i, armed):
        self.k, self.seq, self.armed, self.busy = partition(i), 0, armed, False
        self.pid = part(self.k, PARTITIONS[EVENTS])
        self.cam = f"cam-{random.randrange(16 ** 6):06x}"


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), EVENTS, RAISED)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, EVENTS, topic_partitions_count=PARTITIONS[EVENTS],
                                                   mode=cfg)
    start_ns = time.time_ns()
    aps = [AP(i, random.random() < a.armed_share) for i in range(a.partitions)]
    delay_ns = int(a.entry_delay * 1e9)
    due, tie = [], itertools.count()        # (monotonic t, tie, kind, partition index, entry key)
    entries = {}                            # (partition, cause seq) -> deadline ns: arrivals that will disarm in time
    alarms = {}                             # (partition, cause seq) -> (kind, due ns): alarms that must be raised
    disarm_for = {}                         # partition index -> (partition, cause seq) of its running arrival
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    seen = set()                            # (partition, cause) the monitoring agents already have

    def reader(name):
        def on_alarm(m):
            d = load(m.payload())
            if d["due_ns"] > start_ns:
                lat.add((time.time_ns() - d["due_ns"]) / 1e6)   # live alarms only, not ones replayed at start-up
            key = (d["ap"], d["cause"])
            if name != "monitoring-agents" or key in seen:
                return                      # repeated alarm (at-least-once): already counted
            seen.add(key)
            if key in entries:
                entries.pop(key)
                stats["false_alarms"] += 1  # this arrival disarmed in time; the disarm reached the state too late
            elif key in alarms:
                kind, _ = alarms.pop(key)
                stats[f"{kind}_alarms"] += 1
        return on_alarm

    async def respond(name):
        # One connection per responder, reading the alarms partition directly from its own position (no consumer
        # group, nothing stored on the broker per poll); a full batch means more is waiting, so no pause.
        c, cb = await own_client(), reader(name)
        who, nxt = Consumer.Single(f"{name}-{os.getpid()}"), None
        while True:
            ms = await c.poll_messages(STREAM, RAISED, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    cb(m)
            if len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)

    def pick(want_armed, tries=20):
        for _ in range(tries):
            x = aps[random.randrange(a.partitions)]
            if x.armed == want_armed and not x.busy:
                return x
        return None

    index = {x.k: i for i, x in enumerate(aps)}
    gc.freeze()                             # keep start-up objects out of the collector's way

    def at(t, kind, i, key=None):
        heapq.heappush(due, (t, next(tie), kind, i, key))

    async def sites_loop():
        batches = defaultdict(list)

        def ev(x, kind, **f):
            x.seq += 1
            batches[x.pid].append(SendMessage(msg(ap=x.k, seq=x.seq, k=kind, **f)))
            stats["events"] += 1
            return x.seq

        async def flush():
            # One batch per log partition, all sent at once: per-partition order is kept, one round trip per tick.
            if batches:
                await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                       for p, ms in batches.items()))
                batches.clear()

        async with producer:
            for n, x in enumerate(aps):     # start-up snapshot: every partition's state, sequence 1
                ev(x, ARM if x.armed else DISARM, by="snapshot", snap=1)
                if n % 2000 == 1999:
                    await flush()
            await flush()
            t0 = tick = last = time.monotonic()
            owed = defaultdict(float)
            while True:
                now = time.monotonic()
                x_peak = a.peak if a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1
                dt, last = now - last, now
                owed["bg"] += a.partitions * (1 - a.armed_share) / a.detect_every_s * dt
                owed["arrive"] += a.arrivals_per_s * x_peak * dt
                owed["intrude"] += a.intrusions_per_s * dt
                owed["panic"] += a.panics_per_s * dt
                while owed["bg"] >= 1:      # people moving about in disarmed partitions
                    owed["bg"] -= 1
                    x = pick(False, 4)
                    if x:
                        ev(x, DETECT, trig=random.choice(TRIGGERS), cam=x.cam, delay_s=a.entry_delay)
                        stats["background"] += 1
                while owed["arrive"] >= 1:  # someone arrives at an armed partition and will disarm in time
                    owed["arrive"] -= 1
                    x = pick(True)
                    if not x:
                        continue
                    x.busy = True
                    ns = time.time_ns()
                    s = ev(x, DETECT, trig="person", cam=x.cam, delay_s=a.entry_delay, ns=ns)
                    if random.random() < a.tail_share:
                        g = a.entry_delay - random.uniform(a.min_margin, 3.0)   # disarms in the last 3 s
                    else:
                        g = random.uniform(0.1, 0.75) * a.entry_delay
                    entries[(x.k, s)] = ns + delay_ns
                    i = index[x.k]
                    disarm_for[i] = (x.k, s)
                    for _ in range(random.randint(0, 2)):
                        at(now + random.uniform(0, g), "seen", i)
                    at(now + g, "disarm", i)
                    stats["arrivals"] += 1
                while owed["intrude"] >= 1:  # someone is seen in an armed partition and nobody disarms
                    owed["intrude"] -= 1
                    x = pick(True)
                    if not x:
                        continue
                    x.busy = True
                    ns = time.time_ns()
                    s = ev(x, DETECT, trig=random.choice(TRIGGERS), cam=x.cam, delay_s=a.entry_delay, ns=ns)
                    alarms[(x.k, s)] = ("intrusion", ns + delay_ns)
                    i = index[x.k]
                    at(now + 2, "seen", i)
                    at(now + a.entry_delay + random.uniform(60, 240), "resolve", i)   # the incident is closed
                    stats["intrusions"] += 1
                while owed["panic"] >= 1:   # a panic button pressed in a staffed (disarmed) partition
                    owed["panic"] -= 1
                    x = pick(False)
                    if not x:
                        continue
                    x.busy = True
                    ns = time.time_ns()
                    s = ev(x, PANIC, by=random.choice(("command", "mobile_app")), ns=ns)
                    alarms[(x.k, s)] = ("panic", ns)
                    at(now + random.uniform(60, 240), "resolve", index[x.k])
                    stats["panics"] += 1
                while due and due[0][0] <= now:
                    _, _, kind, i, _ = heapq.heappop(due)
                    x = aps[i]
                    if kind == "seen":
                        ev(x, DETECT, trig="person", cam=x.cam, delay_s=a.entry_delay)
                    elif kind == "disarm":
                        ns = time.time_ns()
                        ev(x, DISARM, by=random.choice(DISARM_BY), ns=ns)
                        x.armed = 0
                        key = disarm_for.pop(i)
                        left = (entries.get(key, ns) - ns) / 1e9   # how much of the entry delay was left at the disarm
                        stats["disarmed"] += 1
                        stats["last3"] += left < 3
                        stats["at_compare"] += left < a.compare_s   # same pre-update state as the live pick
                        at(now + random.uniform(a.rearm_after / 2, a.rearm_after * 1.5), "rearm", i)
                    elif kind == "resolve":
                        ev(x, DISARM, by="command")
                        x.armed = 0
                        at(now + random.uniform(a.rearm_after / 2, a.rearm_after * 1.5), "rearm", i)
                    else:                   # rearm: two badges within 5 s, or the evening schedule
                        ev(x, ARM, by=random.choice(("badge_twice", "schedule")))
                        x.armed, x.busy = 1, False
                await flush()
                stats["peak"] = x_peak > 1
                tick += 0.01                # from the due time, so a late tick never bunches events
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[sites] {a.partitions:,} camera partitions ({a.armed_share:.0%} armed at start), entry delay "
              f"{a.entry_delay:g} s, a detection every {a.detect_every_s:g} s per disarmed partition, "
              f"{a.arrivals_per_s:g} arrivals/s, {a.intrusions_per_s:g} intrusions/s, {a.panics_per_s:g} panic presses/s, "
              f"{a.responders} responders", flush=True)
        while True:
            await asyncio.sleep(1)
            now = time.time_ns()
            # An alarm that has not reached the monitoring agents 2 s after it fell due is counted as missing.
            for key, (kind, due_ns) in list(alarms.items()):
                if now - due_ns > 2e9:
                    alarms.pop(key)
                    stats["missing"] += 1
            for key, dl in list(entries.items()):
                if now - dl > 5e9:          # 5 s past its deadline and no alarm: this arrival is settled
                    entries.pop(key)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            print(f"[sites] {int(stats['events']):>6,} events/s ({phase})  arrivals {int(stats['arrivals']):,}, "
                  f"disarmed in time {int(stats['disarmed']):,} (in the last 3 s of the delay {int(stats['last3']):,})  "
                  f"false alarms raised {int(stats['false_alarms']):,} (with a lane {a.compare_s:g} s slower "
                  f"{int(stats['at_compare']):,})  intrusion alarms {int(stats['intrusion_alarms']):,}  "
                  f"panic alarms {int(stats['panic_alarms']):,}  missing {int(stats['missing']):,}  "
                  f"due-to-responders n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  {stalls.read()}",
                  flush=True)
            stats.clear()

    await asyncio.gather(sites_loop(), report(), stalls.watch(), *(respond(n) for n in RESPONDERS[: a.responders]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--partitions", type=int, default=200_000, help="camera partitions (alarm partitions)")
    p.add_argument("--armed-share", type=float, default=0.5, help="share of partitions armed at start")
    p.add_argument("--entry-delay", type=float, default=30, help="seconds from a trigger to the alarm, unless disarmed")
    p.add_argument("--detect-every-s", type=float, default=60, help="seconds between detections per disarmed partition")
    p.add_argument("--arrivals-per-s", type=float, default=20, help="people arriving at armed partitions, per second")
    p.add_argument("--tail-share", type=float, default=0.15, help="share of arrivals that disarm in the last 3 s")
    p.add_argument("--min-margin", type=float, default=0.25, help="closest a disarm comes to the end of the delay, s")
    p.add_argument("--intrusions-per-s", type=float, default=0.2, help="triggers nobody disarms, per second")
    p.add_argument("--panics-per-s", type=float, default=0.05, help="panic presses per second")
    p.add_argument("--rearm-after", type=float, default=900, help="mean seconds until a partition is armed again")
    p.add_argument("--responders", type=int, default=3, help="services reading alarms-raised, each with its own reader")
    p.add_argument("--poll-ms", type=float, default=1, help="responder wait between polls when nothing is waiting")
    p.add_argument("--compare-s", type=float, default=2,
                   help="also count the arrivals a lane this much slower would have turned into false alarms")
    p.add_argument("--peak", type=float, default=1, help="arrival multiplier during the peak (the morning opening)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
