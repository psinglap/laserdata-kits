"""The source of truth: every SKU that Fulfilled by Faire brands store in the Faire-operated warehouse, and the order
service's answer to multi-brand checkouts.

Each stock change (an inbound receipt from a brand, a pick for a shipped order, a cycle-count or damage adjustment, a
reservation by a retailer's order, a cancelled order releasing its units) goes to the fbf-stock topic, keyed by
brand:SKU, with a per-SKU sequence number and the resulting levels (on hand, reserved, available, case pack), never
only a delta.

Checkouts placed on the live view arrive on the fbf-orders topic, one order per retailer cart across several brands.
Each line is reserved if the warehouse still has the units, or marked short. A short line is a line the retailer was
shown as available that the warehouse can no longer fill: the cost of a stale stock view.

--market-x models a Faire Market: for --market-for seconds other retailers' orders arrive --market-x times faster.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import CASE_PACKS, DEFAULT_URL, ORDERS, OUTCOMES, PARTITIONS, STOCK, STREAM, key, load, msg, part_of


async def main(a):
    async def own_client():                 # one connection per writer and reader, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    rng = random.Random(a.seed)
    admin = await own_client()
    cfg = DirectProducerConfig(batch_length=500, linger_time=timedelta(microseconds=200))
    lane = await (await own_client()).producer(STREAM, STOCK, topic_partitions_count=PARTITIONS, mode=cfg)  # replicated by default
    outcome_p = await (await own_client()).producer(STREAM, OUTCOMES, topic_partitions_count=8, mode=cfg)
    if await admin.get_topic(STREAM, ORDERS) is None:       # checkout.py may not have created it yet
        try:
            await admin.create_topic(STREAM, ORDERS, 8)
        except RuntimeError:
            pass                                             # created by checkout.py in the meantime
    orders = await (await own_client()).consumer_group(
        "fbf-order-service", STREAM, ORDERS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))

    keys = [key(b, s) for b in range(a.brands) for s in range(a.skus)]
    case = {k: rng.choice(CASE_PACKS) for k in keys}
    on_hand = {k: case[k] * (0 if rng.random() < 0.08 else rng.randint(1, 12)) for k in keys}
    reserved = {k: 0 for k in keys}
    seq, stats = defaultdict(int), defaultdict(int)
    pending, outcomes = [], []              # changes and outcomes from orders, sent with the next tick's batch

    def change(k, kind, snap=False):
        seq[k] += 1
        return k, SendMessage(msg(k=k, seq=seq[k], on_hand=on_hand[k], reserved=reserved[k],
                                  avail=on_hand[k] - reserved[k], case=case[k], kind=kind, snap=snap))

    async def send(changes):
        # One batch per partition, all sent at once: same per-SKU order, about one round trip per tick.
        batches = defaultdict(list)
        for k, m in changes:
            batches[part_of(k)].append(m)
        await asyncio.gather(*(lane.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))

    async def on_order(m):
        o = load(m.payload())
        filled = short = 0
        for line in o["lines"]:
            k, q = line["k"], line["qty"]
            if on_hand[k] - reserved[k] >= q:
                reserved[k] += q
                filled += 1
                pending.append(change(k, "reserved"))
            else:
                short += 1                  # shown as available at checkout, already gone at the warehouse
        stats["orders"] += 1
        stats["filled"] += filled
        stats["short"] += short
        stats["short_orders"] += 1 if short else 0
        outcomes.append(SendMessage(msg(order=o["order"], filled=filled, short=short)))

    async def snapshot():
        # The starting levels of every SKU, like a changefeed's initial scan (snap=true, not timed as a live change).
        for i in range(0, len(keys), 5000):
            await send([change(k, "snapshot", snap=True) for k in keys[i:i + 5000]])
        print(f"[warehouse] snapshot: {len(keys):,} SKUs from {a.brands:,} brands", flush=True)

    def reserve(k):
        # Another retailer's order, placed elsewhere on Faire, reserves one or two cases.
        q = case[k] * rng.randint(1, 2)
        if on_hand[k] - reserved[k] >= q:
            reserved[k] += q
            return change(k, "reserved")
        return receive(k) if on_hand[k] - reserved[k] < case[k] and rng.random() < 0.3 else None

    def receive(k):
        on_hand[k] += case[k] * rng.randint(4, 12)          # an inbound shipment from the brand, received and put away
        return change(k, "received")

    def one_change(k):
        x = rng.random()
        if x < 0.45:
            return reserve(k)
        if x < 0.75:
            if reserved[k]:
                q = min(reserved[k], case[k] * rng.randint(1, 2))
                reserved[k] -= q                             # picked, packed and shipped
                on_hand[k] -= q
                return change(k, "picked")
            return receive(k)
        if x < 0.85:
            if on_hand[k] > reserved[k]:
                on_hand[k] = max(reserved[k], on_hand[k] - rng.randint(1, case[k]))   # cycle count or damage
                return change(k, "adjusted")
            return None
        if x < 0.92:
            if reserved[k]:
                reserved[k] -= min(reserved[k], case[k])     # a cancelled order releases its units
                return change(k, "released")
            return None
        return receive(k)

    async def churn():
        async with lane, outcome_p:
            await snapshot()
            gc.freeze()                     # keep start-up tables out of the collector's way
            t0 = due = time.monotonic()
            while True:
                t = time.monotonic() - t0
                market = a.market_at and a.market_at <= t < a.market_at + a.market_for
                n = max(1, int(a.rate / 100))
                extra = int(a.rate * (a.market_x - 1) / 100) if market else 0
                changes = [c for c in (one_change(rng.choice(keys)) for _ in range(n)) if c]
                changes += [c for c in (reserve(rng.choice(keys)) for _ in range(extra)) if c]   # Faire Market orders
                changes += pending
                pending[:] = []
                out, outcomes[:] = list(outcomes), []
                sends = [send(changes)] if changes else []
                if out:
                    sends.append(outcome_p.send_with_partitioning(out, Partitioning.balanced()))
                await asyncio.gather(*sends)
                stats["changes"] += len(changes)
                due += 0.01                 # scheduled from its due time, so a slow tick doesn't drift the rest
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            lines = stats["filled"] + stats["short"]
            rate = 100 * stats["short"] / lines if lines else 0
            print(f"[warehouse] stock changes {stats['changes']:,}  checkout orders {stats['orders']:,}  "
                  f"lines reserved {stats['filled']:,}  short at the warehouse {stats['short']:,} ({rate:.3f}%)  "
                  f"orders with a short line {stats['short_orders']:,}  (this second)", flush=True)
            stats.clear()

    await asyncio.gather(churn(), orders.consume_messages(on_order, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--brands", type=int, default=500, help="brands stored in the FBF warehouse")
    p.add_argument("--skus", type=int, default=150, help="SKUs per brand")
    p.add_argument("--rate", type=int, default=200, help="stock changes per second, all brands")
    p.add_argument("--market-x", type=float, default=1.0, help="order rate multiplier during a Faire Market")
    p.add_argument("--market-at", type=float, default=0, help="seconds after start")
    p.add_argument("--market-for", type=float, default=120)
    p.add_argument("--seed", type=int, default=7)
    asyncio.run(main(p.parse_args()))
