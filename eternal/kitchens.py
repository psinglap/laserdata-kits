"""The source of truth: every restaurant's kitchen, and the check on every FPT prediction.

Each restaurant accepts the orders that fpt.py checks out, cooks them (slower when more orders
are cooking than the kitchen has slots), marks them ready with the Food Order Ready button,
and hands them to a delivery partner. Every change goes to the kitchen-state topic, keyed by
restaurant, with a per-restaurant sequence number and the resulting state: orders cooking,
orders waiting at the counter, the five most recent running orders and the last five completed
preparation times. Those are the inputs Zomato's FPT posts describe.

Every order arrives carrying the prediction made for it and the kitchen sequence the model
saw. If the kitchen had already changed before that prediction was made, the prediction was
made on a stale kitchen view: the cost of a slow lane.
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, KITCHEN, KITCHEN_PARTITIONS, ORDER_PARTITIONS, ORDERS, STREAM, GcPauses, Popularity, load, msg, part


class Kitchen:
    __slots__ = ("seq", "slots", "base", "cooking", "counter", "done", "hist")

    def __init__(self, name):
        rnd = random.Random(name)                   # the same kitchen every run
        self.seq, self.slots, self.base = 0, rnd.randint(2, 6), rnd.uniform(8, 20)   # parallel dishes, minutes per order
        self.cooking, self.counter = {}, set()      # order -> accepted at (monotonic s); orders ready, awaiting pickup
        self.done = deque(maxlen=5)                 # last five completed preparation times, minutes
        self.hist = deque(maxlen=64)                # (ns, seq) of recent changes, to check predictions


async def connected(url):
    # One connection per reader and writer, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def main(a):
    M = a.minute                                    # wall-clock seconds per kitchen minute
    client = await connected(a.url)
    lane = await client.producer(STREAM, KITCHEN, topic_partitions_count=KITCHEN_PARTITIONS,
                                 mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200)))
    if await client.get_topic(STREAM, ORDERS) is None:      # fpt.py may not have created it yet
        try:
            await client.create_topic(STREAM, ORDERS, ORDER_PARTITIONS)
        except RuntimeError:
            pass                                             # created by fpt.py in the meantime
    orders = await (await connected(a.url)).consumer_group(
        "kitchens", STREAM, ORDERS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))

    pop = Popularity(a.restaurants, a.skew)
    kitchens = {name: Kitchen(name) for name in pop.names}
    timers, pending, stats = [], defaultdict(list), defaultdict(int)
    gc_pause = GcPauses()

    def change(r, ev, order):
        # Publish the kitchen's resulting state, never only the delta.
        k = kitchens[r]
        k.seq += 1
        now = time.time_ns()
        k.hist.append((now, k.seq))
        pending[part(r, KITCHEN_PARTITIONS)].append(SendMessage(msg(
            r=r, seq=k.seq, ev=ev, order=order, cooking=len(k.cooking), counter=len(k.counter),
            running=list(k.cooking)[-5:], done=list(k.done), ns=now)))
        stats["events"] += 1

    def later(minutes, kind, r, order):
        heapq.heappush(timers, (time.monotonic() + minutes * M, kind, r, order))

    def accept(r, order):
        k = kitchens[r]
        k.cooking[order] = time.monotonic()
        busy = max(0, len(k.cooking) - k.slots)                     # orders beyond the kitchen's slots wait their turn
        later(k.base * random.uniform(0.85, 1.25) + a.queue_min * busy, "ready", r, order)
        stats["accepted"] += 1
        change(r, "order.accepted", order)

    def ready(r, order):
        k = kitchens[r]
        t0 = k.cooking.pop(order, None)
        if t0 is None:
            return
        k.counter.add(order)
        k.done.append(round((time.monotonic() - t0) / M, 1))     # true FPT, accepted to Food Order Ready
        later(random.uniform(0.5, 4.0), "picked", r, order)        # the delivery partner hands over at the counter
        change(r, "food.ready", order)

    def picked(r, order):
        kitchens[r].counter.discard(order)
        change(r, "order.picked_up", order)

    async def flush():
        batches = {pid: ms for pid, ms in pending.items() if ms}
        pending.clear()
        t = time.monotonic()
        await asyncio.gather(*(lane.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))
        stats["slowest_send_ms"] = max(stats["slowest_send_ms"], int((time.monotonic() - t) * 1000))

    async def snapshot():
        # The start of a dinner rush: orders already cooking, then one state message per kitchen (seq 1).
        for _ in range(a.in_kitchen):
            r = pop.pick()
            k = kitchens[r]
            o = f"OPEN-{stats['open']}"
            stats["open"] += 1
            k.cooking[o] = time.monotonic() - random.uniform(0, k.base) * M
            later(random.uniform(0, k.base), "ready", r, o)
        for i, r in enumerate(pop.names):
            change(r, "kitchen.snapshot", None)
            if i % 5000 == 4999:
                await flush()
        await flush()
        stats["events"] = 0
        gc.freeze()        # the kitchens built at startup live all run; keep them out of later GC passes
        print(f"[kitchens] snapshot: {a.restaurants:,} kitchens, {a.in_kitchen:,} orders already cooking", flush=True)

    async def on_order(m):
        o = load(m.payload())
        r, k = o["r"], kitchens[o["r"]]
        # Had this kitchen changed before the model made its prediction? Then the model never saw that change.
        missed = sum(1 for ns, seq in k.hist if ns <= o["pred_ns"] and seq > o["seen"])
        stats["checked"] += 1
        stats["stale"] += missed > 0
        later(random.uniform(0.2, 1.0), "accept", r, o["order"])   # the restaurant taps accept

    async def kitchen_clock():
        async with lane:
            await snapshot()
            act = {"accept": accept, "ready": ready, "picked": picked}
            while True:
                now = time.monotonic()
                while timers and timers[0][0] <= now:
                    _, kind, r, order = heapq.heappop(timers)
                    act[kind](r, order)
                await flush()
                await asyncio.sleep(0.005)

    async def report():
        while True:
            await asyncio.sleep(1)
            n, stale = stats["checked"], stats["stale"]
            stats["total_checked"] += n
            stats["total_stale"] += stale
            pct = 100 * stale / n if n else 0
            cooking = sum(len(k.cooking) for k in kitchens.values())
            print(f"[kitchens] {stats['events']:>6,} kitchen events/s  predictions checked {n:>5,}/s  "
                  f"made on a stale kitchen view {pct:6.3f}%  (all: {stats['total_stale']:,} of {stats['total_checked']:,})  "
                  f"orders accepted {stats['accepted']:,}  cooking {cooking:,}  slowest send {stats['slowest_send_ms']} ms  "
                  f"longest GC pause {gc_pause.take():.0f} ms", flush=True)
            stats["events"] = stats["checked"] = stats["stale"] = stats["slowest_send_ms"] = 0

    await asyncio.gather(kitchen_clock(), orders.consume_messages(on_order, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--restaurants", type=int, default=150_000)
    p.add_argument("--skew", type=float, default=0.6, help="how strongly orders concentrate on busy kitchens")
    p.add_argument("--in-kitchen", type=int, default=120_000, help="orders already cooking at the start")
    p.add_argument("--queue-min", type=float, default=1.5, help="extra minutes per order beyond the kitchen's slots")
    p.add_argument("--minute", type=float, default=60, help="wall-clock seconds per kitchen minute (1 = 60x faster)")
    asyncio.run(main(p.parse_args()))
