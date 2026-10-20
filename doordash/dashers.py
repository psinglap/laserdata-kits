"""The source of truth: every store's Dashers, and the check on every dispatch decision.

Takes the decisions dispatch.py makes, and plays them out: a Dasher is assigned (now, for
delivery speed, or held back to batch), drives to the store, waits at the counter if the order
is not ready, picks it up and delivers it. Every change at a store goes to the store-feedback
topic, keyed by store, with a per-store sequence and the resulting feedback: Dashers heading
there, Dashers waiting at the counter, the last five Dasher waits, and the utilization of the
store's market (busy Dashers over Dashers on shift). Those are the two signals DoorDash's
dispatch post names for its RL agents: merchant congestion and courier utilization.

Every decision arrives carrying the run time and the feedback sequence the coordinator saw for
that store. If the store's feedback had already changed before that run, the decision was made
on stale feedback; if the newer feedback would have flipped it between speed and batching, it
counts as flipped. That is the cost of a slow lane.
"""
import argparse
import asyncio
import gc
import heapq
import itertools
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DECISION_PARTITIONS, DECISIONS, DEFAULT_URL, FEEDBACK, FEEDBACK_PARTITIONS, STREAM, GcPauses,
                    Popularity, load, market, msg, part, policy)


class Store:
    __slots__ = ("seq", "market", "base", "enroute", "waiting", "ready", "waits", "hist")

    def __init__(self, name, markets):
        rnd = random.Random(name)                   # the same store every run
        self.seq, self.market, self.base = 0, market(name, markets), rnd.uniform(6, 18)   # minutes to prepare an order
        self.enroute = 0                            # Dashers assigned and driving to the store
        self.waiting, self.ready = {}, set()        # order -> Dasher arrived at (monotonic s); orders ready, no Dasher yet
        self.waits = deque(maxlen=5)                # last five Dasher waits at the counter, minutes
        self.hist = deque(maxlen=64)                # (ns, seq, feedback) of recent changes, to check decisions


async def connected(url):
    # One connection per reader and writer, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def main(a):
    M = a.minute                                    # wall-clock seconds per marketplace minute
    client = await connected(a.url)
    lane = await client.producer(STREAM, FEEDBACK, topic_partitions_count=FEEDBACK_PARTITIONS,
                                 mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200)))
    if await client.get_topic(STREAM, DECISIONS) is None:   # dispatch.py may not have created it yet
        try:
            await client.create_topic(STREAM, DECISIONS, DECISION_PARTITIONS)
        except RuntimeError:
            pass                                             # created by dispatch.py in the meantime
    decisions = await (await connected(a.url)).consumer_group(
        "dashers", STREAM, DECISIONS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))

    pop = Popularity(a.stores, a.skew)
    stores = {name: Store(name, a.markets) for name in pop.names}
    pool = max(1, a.dashers // a.markets)           # Dashers on shift per market
    busy = [0] * a.markets
    timers, pending, stats, tie = [], defaultdict(list), defaultdict(int), itertools.count()
    recent_waits = deque(maxlen=2000)
    gc_pause = GcPauses()

    def util(m):
        return min(100, round(100 * busy[m] / pool))

    def change(r, ev, order):
        # Publish the store's resulting feedback, never only the delta.
        s = stores[r]
        s.seq += 1
        now = time.time_ns()
        fb = {"r": r, "seq": s.seq, "ev": ev, "order": order,
              "enroute": s.enroute, "waiting": len(s.waiting),
              "waits": list(s.waits), "util": util(s.market), "ns": now}
        s.hist.append((now, s.seq, fb))
        pending[part(r, FEEDBACK_PARTITIONS)].append(SendMessage(msg(**fb)))
        stats["events"] += 1

    def later(minutes, kind, r, order):
        heapq.heappush(timers, (time.monotonic() + minutes * M, next(tie), kind, r, order))

    def start(r, order, mode, hold=0.0):
        s = stores[r]
        prep = s.base * random.uniform(0.8, 1.3)
        travel = random.uniform(3, 9)
        later(prep + hold, "food", r, order)
        # Speed: a Dasher now. Batch: hold the Dasher back so the arrival lands nearer the food.
        wait_for = random.uniform(0.1, 0.4) if mode == "speed" else max(0.3, (prep - travel) * 0.8)
        later(hold + wait_for, "assign", r, (order, travel))
        stats["orders"] += 1

    def assign(r, payload):
        order, travel = payload
        s = stores[r]
        s.enroute += 1
        busy[s.market] += 1
        later(travel, "arrive", r, order)
        change(r, "dasher.assigned", order)

    def arrive(r, order):
        s = stores[r]
        s.enroute -= 1
        if order in s.ready:                        # the food was waiting for the Dasher: no wait at all
            s.ready.discard(order)
            pickup(r, order, 0.0)
        else:
            s.waiting[order] = time.monotonic()
            change(r, "dasher.arrived", order)

    def food(r, order):
        s = stores[r]
        t0 = s.waiting.pop(order, None)
        if t0 is None:
            s.ready.add(order)                      # ready before the Dasher got there
        else:
            pickup(r, order, (time.monotonic() - t0) / M)

    def pickup(r, order, waited):
        s = stores[r]
        s.waits.append(round(waited, 1))
        recent_waits.append(waited)
        later(random.uniform(8, 18), "deliver", r, order)
        change(r, "order.picked_up", order)

    def deliver(r, order):
        busy[stores[r].market] -= 1
        stats["delivered"] += 1

    def release(r, order):
        busy[stores[r].market] -= 1                 # a Dasher on an earlier delivery comes free

    async def flush():
        batches = {pid: ms for pid, ms in pending.items() if ms}
        pending.clear()
        t = time.monotonic()
        await asyncio.gather(*(lane.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))
        stats["slowest_send_ms"] = max(stats["slowest_send_ms"], int((time.monotonic() - t) * 1000))

    async def snapshot():
        # A dinner rush already running: Dashers out on deliveries, orders in flight, then one feedback message per store (seq 1).
        for _ in range(a.in_flight):
            r = pop.pick()
            busy[stores[r].market] += 1
            later(random.uniform(0, 20), "release", r, None)
        for i in range(a.in_flight // 4):
            r = pop.pick()
            start(r, f"OPEN-{i}", "speed", hold=random.uniform(0, 3))
        for i, r in enumerate(pop.names):
            change(r, "store.snapshot", None)
            if i % 5000 == 4999:
                await flush()
        await flush()
        stats["events"] = stats["orders"] = 0
        gc.freeze()        # the stores built at startup live all run; keep them out of later GC passes
        print(f"[dashers] snapshot: {a.stores:,} stores in {a.markets:,} markets, {pool * a.markets:,} Dashers on shift, "
              f"{a.in_flight:,} already on deliveries", flush=True)

    async def on_decision(m):
        d = load(m.payload())
        r, s = d["r"], stores[d["r"]]
        # Had this store's feedback changed before the run? Then the coordinator decided without that change.
        newest = None
        for ns, seq, fb in s.hist:
            if ns <= d["run_ns"] and seq > d["seen"]:
                newest = fb
        stats["checked"] += 1
        if newest is not None:
            stats["stale"] += 1
            stats["flipped"] += policy(newest)[0] != d["mode"]
        start(r, d["order"], d["mode"])

    async def clock():
        async with lane:
            await snapshot()
            act = {"assign": assign, "arrive": arrive, "food": food, "deliver": deliver, "release": release}
            while True:
                now = time.monotonic()
                while timers and timers[0][0] <= now:
                    _, _, kind, r, order = heapq.heappop(timers)
                    act[kind](r, order)
                await flush()
                await asyncio.sleep(0.005)

    async def report():
        while True:
            await asyncio.sleep(1)
            n, stale, flipped = stats["checked"], stats["stale"], stats["flipped"]
            for k in ("checked", "stale", "flipped"):
                stats["total_" + k] += stats[k]
            pct = 100 * stale / n if n else 0
            fpct = 100 * flipped / n if n else 0
            w = sorted(recent_waits)
            med_wait = w[len(w) // 2] if w else 0.0
            u = sum(min(100, 100 * b / pool) for b in busy) / len(busy)
            print(f"[dashers] {stats['events']:>6,} feedback events/s  decisions checked {n:>5,}/s  "
                  f"made on stale store feedback {pct:6.3f}%  flipped speed/batch {fpct:6.3f}%  "
                  f"(all: {stats['total_stale']:,} stale, {stats['total_flipped']:,} flipped of {stats['total_checked']:,})  "
                  f"orders {stats['orders']:,}  delivered {stats['delivered']:,}  median Dasher wait {med_wait:.1f} min  "
                  f"utilization {u:.0f}%  slowest send {stats['slowest_send_ms']} ms  longest GC pause {gc_pause.take():.0f} ms",
                  flush=True)
            for k in ("events", "checked", "stale", "flipped", "slowest_send_ms", "orders", "delivered"):
                stats[k] = 0

    await asyncio.gather(clock(), decisions.consume_messages(on_decision, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--stores", type=int, default=200_000)
    p.add_argument("--markets", type=int, default=2_000, help="local markets; each store belongs to one")
    p.add_argument("--skew", type=float, default=0.6, help="how strongly orders concentrate on busy stores")
    p.add_argument("--dashers", type=int, default=200_000, help="Dashers on shift, split evenly across markets")
    p.add_argument("--in-flight", type=int, default=120_000, help="Dashers already on deliveries at the start")
    p.add_argument("--minute", type=float, default=60, help="wall-clock seconds per marketplace minute (1 = 60x faster)")
    asyncio.run(main(p.parse_args()))
