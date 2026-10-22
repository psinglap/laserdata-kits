"""Outbound docks at hubs and gateways on a festive-peak shift, and the handhelds and screens that read every alert.

Plays --docks docks in one process. Each dock loads one outbound trailer (trip) at a time, with --loaders loaders
scanning a bag or loose shipment every few seconds. Everything goes to dock-scans, keyed by trip, with the trip's
event sequence:
  * plan: the trip's destinations, when the trip opens and whenever linehaul planning revises it (the whole
    list each time);
  * load: one scan per bag or loose shipment, with its destination, the loader and the trip's load count;
  * seal: the trailer is closed once its load is on; a new trip opens at the dock a little later.
Misloads happen two ways (rates illustrative; set them to yours): a bag from the wrong cart (--wrong-p), and plan
revisions that move a destination to another trailer while the loaders keep loading by the old plan until the
news reaches them (--told-after seconds).

This script knows every misload (the truth) and checks every alert the load check publishes. --notifiers readers
play the services that act on dock-alerts (the loader's handheld, the dock supervisor's screen, the yard view),
each on its own connection, reading the alerts partition directly from its own position.

The business number: of the misloads, how many were flagged on the loader's handheld before that loader's next
scan (the bag is still at the trailer door) and before the trailer was sealed, how many alerts were false, and
how many misloads were never flagged. For comparison: how many a lane --compare-s seconds behind would still have
flagged before the loader's next scan. Prints it every second, with scan-to-handheld p50/p99 (both hops: scan, load check,
alert, handheld).
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

from common import (ALERTS, DEFAULT_URL, DESTS, DOCK, MISLOAD, ORIGINS, PARTITIONS, STREAM, Stalls, Window,
                    ensure_topics, load, msg, part, trip_id)

NOTIFIERS = ["loader-handheld", "dock-supervisor", "yard-view", "control-tower", "client-tracking"]


class Trip:
    __slots__ = ("id", "seq", "plan", "old", "told_at", "rev", "loaded", "target", "full", "sealed", "seal_t", "misloads")

    def __init__(self, tid, plan, target, loaded):
        self.id, self.seq, self.plan, self.old, self.told_at, self.rev = tid, 0, plan, plan, 0.0, 1
        self.loaded, self.target, self.full, self.sealed, self.seal_t = loaded, target, False, False, 0.0
        self.misloads = []


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), DOCK, ALERTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, DOCK, topic_partitions_count=PARTITIONS[DOCK], mode=cfg)
    start_ns = time.time_ns()
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    origin = [ORIGINS[i % len(ORIGINS)] for i in range(a.docks)]
    trips = [None] * a.docks                # the trip loading at each dock, or None between trailers
    scans = defaultdict(int)                # loader -> scans so far (an alert is "in time" if this has not moved)
    truth = {}                              # (trip, scan seq) -> [loader, scans at that scan, trip, scan time, alerted]
    serial = defaultdict(int)
    done = set()                            # misloads already audited after their seal
    last_mis = {}                           # loader -> its latest misload, until that loader's next scan
    due, out = [], defaultdict(list)        # out: partition -> messages for this tick, in creation order
    tie = itertools.count()                 # keeps heap entries with the same due time in push order

    def at(t, kind, i, x):
        heapq.heappush(due, (t, next(tie), kind, i, x))

    def emit(tr, **d):
        tr.seq += 1
        out[part(tr.id, PARTITIONS[DOCK])].append(SendMessage(msg(t=tr.id, seq=tr.seq, **d)))
        return tr.seq

    def open_trip(i, now, mid_shift=False):
        o = origin[i]
        serial[o] += 1
        target = random.randint(*a.load)
        plan = random.sample(DESTS, random.randint(*a.plan_dests))
        tr = Trip(trip_id(o, serial[o]), plan, target,
                  random.randint(0, target - 1) if mid_shift else 0)
        trips[i] = tr
        emit(tr, k="plan", dests=plan, rev=1, dock=i)
        if random.random() < a.revise_p:    # linehaul planning revises this trip partway through loading
            left = (tr.target - tr.loaded) / (a.loaders / sum(a.scan_s) * 2)
            at(now + random.uniform(0.15, 0.7) * left, "revise", i, tr.id)

    def revise(tr, now):
        old = list(tr.plan)
        if len(tr.plan) > 1 and random.random() < 0.7:  # a destination moves to another trailer
            moved = random.choice(tr.plan)
            tr.plan = [d for d in tr.plan if d != moved]
        else:                                            # a destination is added to this one
            tr.plan = tr.plan + [random.choice([d for d in DESTS if d not in tr.plan])]
        tr.old, tr.told_at, tr.rev = old, now + random.uniform(*a.told_after), tr.rev + 1
        emit(tr, k="plan", dests=tr.plan, rev=tr.rev)
        stats["revisions"] += 1

    def scan(i, j, now):
        tr = trips[i]
        if tr is None or tr.full:
            return                          # between trailers, or this one is loaded and closing
        ldr = f"{origin[i]}-D{i:04d}-L{j}"
        follow = tr.old if now < tr.told_at else tr.plan  # loaders load by the plan they know
        if random.random() < a.wrong_p:
            dest = random.choice([d for d in DESTS if d not in tr.plan])   # a bag from the wrong cart
        else:
            dest = random.choice(follow)
        tr.loaded += 1
        scans[ldr] += 1
        prev = last_mis.pop(ldr, None)
        if prev is not None and now - prev[3] > a.compare_s:
            stats["at_compare"] += 1        # a lane this slow would still have flagged it before this scan
        seq = emit(tr, k="load", bag=f"BG{random.randrange(10**9):09d}", dest=dest, ldr=ldr, n=tr.loaded)
        stats["scans"] += 1
        if dest not in tr.plan:
            truth[(tr.id, seq)] = last_mis[ldr] = [ldr, scans[ldr], tr, now, False]
            tr.misloads.append(seq)
            stats["misloads"] += 1
        if tr.loaded >= tr.target:          # the load is on: doors closed and paperwork done, then the seal scan
            tr.full = True
            at(now + random.uniform(*a.seal_s), "seal", i, tr)

    def seal(i, tr, now):
        emit(tr, k="seal", n=tr.loaded)
        tr.sealed, tr.seal_t = True, now
        stats["seals"] += 1
        for s in tr.misloads:
            t = truth[(tr.id, s)]
            if last_mis.get(t[0]) is t:     # the loader's last bag on this trailer: the seal is the deadline
                del last_mis[t[0]]
                stats["at_compare"] += now - t[3] > a.compare_s
        at(now + a.audit_s, "audit", i, tr)
        at(now + random.uniform(*a.gap_s), "open", i, None)

    def audit(tr):
        # a few seconds after the seal: any misload on that trailer that was never flagged is missed
        for s in tr.misloads:
            done.add((tr.id, s))
            if not truth.pop((tr.id, s))[4]:
                stats["missed"] += 1

    def reader(name):
        def on_alert(m):
            d = load(m.payload())
            if d["src_ns"] > start_ns:
                lat.add((time.time_ns() - d["src_ns"]) / 1e6)   # live alerts only, not ones replayed at start-up
            if name != "loader-handheld":
                return
            if d["src_ns"] <= start_ns:
                return                      # from an earlier run of this script
            t = truth.get((d["t"], d["scan_seq"]))
            if t is None:
                if (d["t"], d["scan_seq"]) in done:
                    return                  # re-sent by a restarted load check after its trailer left: a repeat
                stats["false"] += 1         # an alert for a bag that was loaded correctly
                return
            if t[4]:
                return                      # repeated alert (at-least-once): already counted
            t[4] = True
            stats["flagged"] += 1
            if scans[t[0]] == t[1]:
                stats["before_next"] += 1   # the loader has not scanned another bag: this one is still at the door
            if not t[2].sealed:
                stats["before_seal"] += 1   # the trailer is still open: the bag comes off before it leaves
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

    async def dock_loop():
        t0 = tick = time.monotonic()
        for i in range(a.docks):            # start mid-shift: every dock has a trailer partly loaded
            open_trip(i, t0, mid_shift=True)
            for j in range(a.loaders):
                at(t0 + random.uniform(0, a.scan_s[1]), "scan", i, j)
        gc.freeze()                         # keep start-up objects out of the collector's way
        async with producer:
            while True:
                now = time.monotonic()
                x_peak = a.peak if a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1
                while due and due[0][0] <= now:
                    t, _, kind, i, x = heapq.heappop(due)
                    if kind == "scan":
                        scan(i, x, now)
                        # from its due time, so a late tick never bunches scans; faster loading in the peak
                        at(t + random.uniform(*a.scan_s) / x_peak, "scan", i, x)
                    elif kind == "revise":
                        if trips[i] is not None and trips[i].id == x and not trips[i].full:
                            revise(trips[i], now)
                    elif kind == "seal":
                        seal(i, x, now)
                    elif kind == "open":
                        open_trip(i, now)
                    else:
                        audit(x)
                # One batch per partition, all sent at once: per-trip order is kept, one round trip per tick.
                if out:
                    batches = list(out.items())
                    out.clear()
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches))
                stats["peak"] = x_peak > 1
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[docks] {a.docks:,} docks across {', '.join(ORIGINS)}, {a.loaders} loaders each, a scan every "
              f"{a.scan_s[0]:g}-{a.scan_s[1]:g} s per loader, {a.load[0]}-{a.load[1]} bags per trailer, "
              f"{a.revise_p:.0%} of trips revised while loading, {a.notifiers} notifiers", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            print(f"[docks] {int(stats['scans']):>6,} scans/s ({phase})  seals {int(stats['seals']):,}  plan revisions "
                  f"{int(stats['revisions']):,}  misloads {int(stats['misloads']):,}  flagged at the loader's handheld "
                  f"{int(stats['flagged']):,}: before the loader's next scan {int(stats['before_next']):,}, before "
                  f"the seal {int(stats['before_seal']):,} (a lane {a.compare_s:g} s behind: before the next scan "
                  f"{int(stats['at_compare']):,})  false alerts {int(stats['false']):,}  missed "
                  f"{int(stats['missed']):,}  scan-to-handheld n {r['n']:,} p50 {r['p50']:.1f}ms "
                  f"p99 {r['p99']:.1f}ms  {stalls.read()}", flush=True)
            peak = stats["peak"]
            stats.clear()
            stats["peak"] = peak

    await asyncio.gather(dock_loop(), report(), stalls.watch(), *(notify(n) for n in NOTIFIERS[: a.notifiers]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--docks", type=int, default=1000, help="outbound docks loading at once, across the network")
    p.add_argument("--loaders", type=int, default=2, help="loaders scanning at each dock")
    p.add_argument("--scan-s", type=float, nargs=2, default=[3, 5], help="seconds between one loader's scans")
    p.add_argument("--load", type=int, nargs=2, default=[80, 200], help="bags and loose shipments per trailer")
    p.add_argument("--plan-dests", type=int, nargs=2, default=[1, 3], help="destinations planned per trailer")
    p.add_argument("--revise-p", type=float, default=0.1, help="share of trips linehaul planning revises while loading")
    p.add_argument("--told-after", type=float, nargs=2, default=[5, 30],
                   help="seconds before loaders load by a revised plan")
    p.add_argument("--wrong-p", type=float, default=0.001, help="chance a scanned bag came from the wrong cart")
    p.add_argument("--seal-s", type=float, nargs=2, default=[30, 90], help="seconds from the last scan to the seal")
    p.add_argument("--gap-s", type=float, nargs=2, default=[30, 120], help="seconds from a seal to the next trip")
    p.add_argument("--audit-s", type=float, default=3, help="seconds after a seal to count unflagged misloads")
    p.add_argument("--notifiers", type=int, default=3, help="services reading dock-alerts, each with its own reader")
    p.add_argument("--poll-ms", type=float, default=1, help="notifier wait between polls when nothing is waiting")
    p.add_argument("--compare-s", type=float, default=5,
                   help="also count misloads a lane this slow would still have flagged before the loader's next scan")
    p.add_argument("--peak", type=float, default=1, help="scan-rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
