"""Connected carrier fleets, the shipments put on their trucks, and the customer-facing services that read every
shared update.

Plays --vehicles trucks in one process. Carriers share their whole fleet; every truck sends a position every
--ping-s seconds whether or not it carries a customer's shipment. Everything goes to fleet-positions, keyed by
vehicle, with the truck's event sequence:
  * assign: a customer's shipment is put on this truck (pick-up and delivery locations, time window);
  * pos: the truck's position;
  * stop: the shipment is no longer on this truck before delivery (the carrier stopped sharing, the load was
    cancelled or moved to another tractor; --stop-p of shipments, illustrative).
A truck drives to the pick-up, loads, drives to the delivery location, unloads, drives off and waits for its next
shipment. Distances are covered at --km-per-ping, so a run can be sped up.

This script applies project44's sharing rule (common.step) to the true state of every truck, so it knows which
customer is entitled to each position (the truth), and checks every update the share gate publishes. --notifiers
readers play the services that act on shipment-updates (the customer push to the shipper's API or webhook, the
visibility screens, the ETA service), each on its own connection, reading the updates partition directly.

The business number: positions shared with the right customer, positions shared with a customer who was not
entitled to them at that moment (a leak), positions a customer was entitled to and never got (missed), and
positions withheld because no shipment was open on that truck. For comparison: how many positions a gate that
learns of stops --compare-s seconds late would have sent to a customer whose shipment had already left the truck.
Prints it every second, with position-to-customer p50/p99 (both hops: position, gate, update, customer push).
"""
import argparse
import asyncio
import gc
import heapq
import itertools
import math
import os
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DONE, DROP_KM, LANE, PARTITIONS, PENDING, SITES, STREAM, TRACKING, UPDATES, DEFAULT_URL, Stalls,
                    Window, ensure_topics, km, load, msg, part, step, vehicle_id)

NOTIFIERS = ["customer-push", "visibility-screens", "eta-service", "exception-alerts"]
SITE_NAMES = list(SITES)
IDLE, TO_PICKUP, LOADING, TO_DROP, UNLOADING, LEAVING = range(6)


class Truck:
    __slots__ = ("id", "seq", "pos", "phase", "target", "dwell", "sh", "c", "state", "stop_at", "pings", "stale_until",
                 "site")

    def __init__(self, tid, seq0, site):
        self.id, self.seq, self.site, self.pos = tid, seq0, site, SITES[site]
        self.phase, self.target, self.dwell, self.sh, self.c, self.state = IDLE, None, 0, None, None, None
        self.stop_at, self.pings, self.stale_until = None, 0, 0.0


def toward(p, q, d):
    """Move d km from p toward q; returns (new point, arrived)."""
    dist = km(p, q)
    if dist <= d:
        return q, True
    f = d / dist
    return (p[0] + (q[0] - p[0]) * f, p[1] + (q[1] - p[1]) * f), False


def offset(p, d, bearing):
    """A point d km from p at the given bearing (radians)."""
    dlat = d * math.cos(bearing) / 111.2
    dlon = d * math.sin(bearing) / (111.2 * math.cos(math.radians(p[0])))
    return p[0] + dlat, p[1] + dlon


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LANE, UPDATES)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, LANE, topic_partitions_count=PARTITIONS[LANE], mode=cfg)
    start_ns = time.time_ns()
    seq0 = start_ns // 1000                 # sequences keep rising across runs, so a retained lane never confuses the gate
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    trucks = [Truck(vehicle_id(i), seq0, random.choice(SITE_NAMES)) for i in range(a.vehicles)]
    truth = {}                              # (vehicle, seq) -> [entitled shipments, received shipments]
    audit_q = deque()                       # (due time, key) in creation order
    seen = [set(), set()]                   # recently audited keys (two generations), to tell late updates apart
    due, out = [], defaultdict(list)        # out: partition -> messages for this tick, in creation order
    tie = itertools.count()                 # keeps heap entries with the same due time in push order

    def at(t, kind, i):
        heapq.heappush(due, (t, next(tie), kind, i))

    def emit(tr, **d):
        tr.seq += 1
        out[part(tr.id, PARTITIONS[LANE])].append(SendMessage(msg(v=tr.id, seq=tr.seq, **d)))
        return tr.seq

    def new_shipment(tr, now, phase, win_ns=None):
        pu, dr = random.sample(SITE_NAMES, 2)
        tr.sh, tr.c = f"SH{random.randrange(10 ** 10):010d}", f"CUST-{random.randrange(a.customers):05d}"
        win = win_ns if win_ns is not None else time.time_ns() + int(random.uniform(*a.window_in) * 1e9)
        tr.state = [PENDING, SITES[pu], SITES[dr], win, None]
        emit(tr, k="assign", sh=tr.sh, c=tr.c, pu=SITES[pu], dr=SITES[dr], win=win)
        tr.phase, tr.pings = phase, 0
        tr.target = SITES[pu] if phase == TO_PICKUP else SITES[dr]
        if phase == LOADING:
            tr.pos, tr.dwell = SITES[pu], random.randint(*a.load_pings)
        trip = (km(tr.pos, SITES[pu]) + km(SITES[pu], SITES[dr])) / a.km_per_ping + 8
        tr.stop_at = random.randint(1, int(trip)) if random.random() < a.stop_p else None
        stats["assigned"] += 1

    def move(tr):
        d = a.km_per_ping * random.uniform(0.8, 1.2)
        if tr.phase in (TO_PICKUP, TO_DROP):
            tr.pos, arrived = toward(tr.pos, tr.target, d)
            if arrived:
                tr.phase = LOADING if tr.phase == TO_PICKUP else UNLOADING
                tr.dwell = random.randint(*(a.load_pings if tr.phase == LOADING else a.unload_pings))
        elif tr.phase in (LOADING, UNLOADING):
            tr.dwell -= 1
            if tr.dwell <= 0:
                if tr.phase == LOADING:
                    tr.phase, tr.target = TO_DROP, tr.state[2]
                else:
                    tr.phase = LEAVING
        elif tr.phase == LEAVING:          # drive off the delivery location, then park and wait for the next load
            tr.pos = offset(tr.pos, max(d, 3 * DROP_KM), random.uniform(0, 2 * math.pi))
            tr.phase = IDLE
            return True
        return False

    def stop(tr, now):
        emit(tr, k="stop", sh=tr.sh, why="stopped")
        stats["stops"] += 1
        if tr.state[0] == TRACKING:         # a gate that hears of this late keeps sharing until it does
            tr.stale_until = now + a.compare_s
        tr.sh = tr.state = tr.stop_at = None
        tr.phase = IDLE

    def ping(i, now):
        tr = trucks[i]
        went_idle = False
        if tr.sh is not None and tr.stop_at is not None and tr.pings >= tr.stop_at and tr.phase != LEAVING:
            stop(tr, now)
            went_idle = True
        else:
            went_idle = move(tr)
            if went_idle and tr.state is not None and tr.state[0] == PENDING:
                # delivered without its tracking window ever opening: sharing times out, as the rule says
                emit(tr, k="stop", sh=tr.sh, why="timeout")
                stats["timeouts"] += 1
                tr.sh = tr.state = tr.stop_at = None
        tr.pings += 1
        ns = time.time_ns()
        jitter = 0.00002 if tr.phase in (IDLE, LOADING, UNLOADING) else 0.0
        lat_, lon_ = round(tr.pos[0] + random.uniform(-jitter, jitter), 6), round(tr.pos[1] + random.uniform(-jitter, jitter), 6)
        entitled = set()
        if tr.state is not None:
            ev, share = step(tr.state, lat_, lon_, ns, a.dwell_s)
            if ev is not None:
                stats[ev] += 1
            if share:
                entitled.add(tr.sh)
            if tr.state[0] == DONE:
                tr.sh = tr.state = tr.stop_at = None
        seq = emit(tr, k="pos", lat=lat_, lon=lon_, ns=ns)
        stats["positions"] += 1
        if entitled:
            stats["entitled"] += 1
        else:
            stats["withheld"] += 1
        if now < tr.stale_until:
            stats["would_leak"] += 1        # a gate still on the stopped shipment would have sent this one
        key = (tr.id, seq)
        truth[key] = [entitled, set()]
        audit_q.append((now + a.audit_s, key))
        if went_idle and tr.phase == IDLE and tr.sh is None:
            at(now + random.uniform(*a.idle_s), "assign", i)

    def audit(now):
        # a few seconds after a position: any entitled customer who never got it is missed
        while audit_q and audit_q[0][0] <= now:
            _, key = audit_q.popleft()
            ent, got = truth.pop(key)
            stats["missed"] += len(ent - got)
            seen[0].add(key)

    def reader(name):
        def on_update(m):
            d = load(m.payload())
            if d["k"] != "pos":
                if name == "customer-push" and d["src_ns"] > start_ns:
                    stats["got_" + d["k"]] += 1
                return
            if d["src_ns"] <= start_ns:
                return                      # from an earlier run of this script
            lat.add((time.time_ns() - d["src_ns"]) / 1e6)
            if name != "customer-push":
                return
            key = (d["v"], d["pseq"])
            t = truth.get(key)
            if t is None:
                if key in seen[0] or key in seen[1]:
                    stats["late"] += 1      # arrived after its audit window: counted as missed already
                return
            if d["sh"] not in t[0]:
                stats["leaks"] += 1         # a position shared with a customer not entitled to it at that moment
                return
            if d["sh"] in t[1]:
                return                      # repeated update (at-least-once): already counted
            t[1].add(d["sh"])
            stats["delivered"] += 1
        return on_update

    async def notify(name):
        # One connection per notifier, reading the updates partition directly from its own position (no consumer
        # group, nothing stored on the broker per poll); a full batch means more is waiting, so no pause.
        c, cb = await own_client(), reader(name)
        who, nxt = Consumer.Single(f"{name}-{os.getpid()}"), None
        while True:
            ms = await c.poll_messages(STREAM, UPDATES, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    cb(m)
            if len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)

    async def fleet_loop():
        t0 = tick = time.monotonic()
        now_ns = time.time_ns()
        for i, tr in enumerate(trucks):     # start mid-day: trucks on their way, loading, in transit or parked
            r = random.random()
            if r < 0.3:
                at(t0 + random.uniform(0, a.idle_s[1]), "assign", i)
            elif r < 0.6:
                new_shipment(tr, t0, TO_PICKUP)
                tr.pos = offset(tr.state[1], random.uniform(5, 120), random.uniform(0, 2 * math.pi))
            elif r < 0.75:
                new_shipment(tr, t0, LOADING, win_ns=now_ns - 60 * 10 ** 9)
            else:                           # just left the pick-up: inside its outer geofence, window open
                new_shipment(tr, t0, TO_DROP, win_ns=now_ns - 60 * 10 ** 9)
                tr.pos, _ = toward(tr.state[1], tr.state[2], random.uniform(0, 30))
            at(t0 + random.uniform(0, a.ping_s), "ping", i)
        gc.freeze()                         # keep start-up objects out of the collector's way
        async with producer:
            while True:
                now = time.monotonic()
                x_peak = a.peak if a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1
                while due and due[0][0] <= now:
                    t, _, kind, i = heapq.heappop(due)
                    if kind == "ping":
                        ping(i, now)
                        # from its due time, so a late tick never bunches pings; more often in the peak
                        at(t + a.ping_s * random.uniform(0.9, 1.1) / x_peak, "ping", i)
                    elif trucks[i].sh is None:
                        new_shipment(trucks[i], now, TO_PICKUP)
                audit(now)
                # One batch per partition, all sent at once: per-truck order is kept, one round trip per tick.
                if out:
                    batches = list(out.items())
                    out.clear()
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches))
                stats["peak"] = x_peak > 1
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[fleet] {a.vehicles:,} trucks, a position every {a.ping_s:g} s each, {a.km_per_ping:g} km per "
              f"position, {a.customers:,} customers, {a.stop_p:.0%} of shipments stopped before delivery, "
              f"{a.notifiers} notifiers", flush=True)
        rotate = time.monotonic()
        while True:
            await asyncio.sleep(1)
            if time.monotonic() - rotate > 30:
                seen.reverse()
                seen[0] = set()
                rotate = time.monotonic()
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            print(f"[fleet] {int(stats['positions']):>6,} positions/s ({phase})  shipments assigned "
                  f"{int(stats['assigned']):,}  tracking started {int(stats['start']):,}  completed "
                  f"{int(stats['end']):,}  stopped {int(stats['stops']):,}  positions customers were entitled to "
                  f"{int(stats['entitled']):,}: shared with the right customer {int(stats['delivered']):,}, leaks "
                  f"{int(stats['leaks']):,}, missed {int(stats['missed']):,}, late {int(stats['late']):,}  "
                  f"withheld {int(stats['withheld']):,} (a gate {a.compare_s:g} s late on stops: "
                  f"{int(stats['would_leak']):,} sent after the shipment left the truck)  position-to-customer "
                  f"n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  {stalls.read()}", flush=True)
            peak = stats["peak"]
            stats.clear()
            stats["peak"] = peak

    await asyncio.gather(fleet_loop(), report(), stalls.watch(), *(notify(n) for n in NOTIFIERS[: a.notifiers]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--vehicles", type=int, default=100000, help="connected trucks, whole fleets shared")
    p.add_argument("--ping-s", type=float, default=60, help="seconds between one truck's positions")
    p.add_argument("--km-per-ping", type=float, default=1.5, help="km a moving truck covers between positions")
    p.add_argument("--customers", type=int, default=1300, help="shippers whose shipments ride on these trucks")
    p.add_argument("--load-pings", type=int, nargs=2, default=[20, 90], help="positions while loading at pick-up")
    p.add_argument("--unload-pings", type=int, nargs=2, default=[20, 90], help="positions while unloading")
    p.add_argument("--idle-s", type=float, nargs=2, default=[600, 7200], help="seconds parked between shipments")
    p.add_argument("--window-in", type=float, nargs=2, default=[0, 1800],
                   help="seconds from assignment until the shipment's tracking window opens")
    p.add_argument("--stop-p", type=float, default=0.02, help="share of shipments stopped before delivery")
    p.add_argument("--dwell-s", type=float, default=3 * 3600, help="delivery geofence dwell before completion")
    p.add_argument("--audit-s", type=float, default=5, help="seconds after a position to count it missed")
    p.add_argument("--notifiers", type=int, default=3, help="services reading shipment-updates, each its own reader")
    p.add_argument("--poll-ms", type=float, default=1, help="notifier wait between polls when nothing is waiting")
    p.add_argument("--compare-s", type=float, default=5,
                   help="also count positions a gate that learns of stops this late would have shared")
    p.add_argument("--peak", type=float, default=1, help="position-rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
