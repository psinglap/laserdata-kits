"""Your order services: every order's state changes, pushed to Meecom as order updates.

Plays the microservices that use Meecom's Kafka-based real-time push today. Each order
moves placed -> confirmed -> shipped -> out_for_delivery -> delivered; every change goes
to the order-updates topic keyed by order ID, with a per-order sequence and the resulting
state (never only a delta). A real order takes days; --step-s compresses it so every
state shows up within a minute.

It is also the truth. It reads Meecom's send receipts and checks each one: was the state
in that notification still the order's current state when Meecom sent it? A "Shipped"
push sent after the order was already out for delivery is out of date, and that is
what a slow lane costs the shopper.
"""
import argparse
import asyncio
import heapq
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, ORDER_UPDATES, PARTITIONS, RECEIPTS, STATES, STREAM, channel, ensure_topics, load,
                    msg, order_id, part)


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, ORDER_UPDATES, RECEIPTS)
    parts = PARTITIONS[ORDER_UPDATES]
    out = await client.producer(STREAM, ORDER_UPDATES, topic_partitions_count=parts,
                                mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200)))
    rc = IggyClient.from_connection_string(a.url)            # receipts on their own connection
    await rc.connect()
    receipts = await rc.consumer_group("order-services-receipts", STREAM, RECEIPTS, polling_strategy=PollingStrategy.Next(),
                                       batch_length=5000, auto_commit=AutoCommit.Interval(timedelta(seconds=1)),
                                       poll_interval=timedelta(milliseconds=1), init_retries=10,
                                       init_retry_interval=timedelta(seconds=1))
    state, changed = {}, {}           # order -> current state index; order -> ns of each state change
    due = []                          # (when the next change is due, order)
    stats = defaultdict(int)
    n_orders = 0

    def change(o):
        state[o] = state.get(o, -1) + 1
        changed.setdefault(o, []).append(time.time_ns())
        s = state[o]
        if s + 1 < len(STATES):
            heapq.heappush(due, (time.monotonic() + a.step_s * random.uniform(0.5, 1.5), o))
        stats["updates"] += 1
        return SendMessage(msg(k=o, seq=s + 1, state=STATES[s], ch=channel(), user=f"U{random.randrange(10**9):09d}"))

    async def send(changes):
        # One batch per partition, all sent at once: per-order order is kept, about one round trip per tick.
        batches = defaultdict(list)
        for o, m in changes:
            batches[part(o, parts)].append(m)
        await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(pid)) for pid, ms in batches.items()))

    async def run():
        nonlocal n_orders
        t0 = tick = time.monotonic()
        owed = 0.0
        async with out:
            while True:
                t = time.monotonic() - t0
                x = a.sale if a.sale_at <= t < a.sale_at + a.sale_for else 1
                owed += a.orders_per_min * x / 60 / 100
                changes = []
                for _ in range(int(owed)):
                    n_orders += 1
                    o = order_id(n_orders)
                    changes.append((o, change(o)))
                    stats["orders"] += 1
                owed -= int(owed)
                now = time.monotonic()
                while due and due[0][0] <= now:
                    _, o = heapq.heappop(due)
                    changes.append((o, change(o)))
                if changes:
                    await send(changes)
                tick = max(tick + 0.01, time.monotonic() - 0.1)     # never bank more than 100 ms of catch-up
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def on_receipt(m):
        r = load(m.payload())
        times = changed.get(r["k"])
        if not times:
            return
        stats["checked"] += 1
        newer = times[r["seq"]] if r["seq"] < len(times) else None     # when the next state happened, if it has
        if newer is not None and newer <= r["sent_ns"]:
            stats["stale"] += 1           # the shopper is told a state the order has already left
        if r["sent_ns"] - times[r["seq"] - 1] <= 1e9:
            stats["within_1s"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            c = stats["checked"]
            stale = 100 * stats["stale"] / c if c else 0
            on_time = 100 * stats["within_1s"] / c if c else 0
            print(f"[orders] {stats['orders']:>6,} new orders/s  {stats['updates']:>6,} order updates/s  "
                  f"receipts checked {c:,}  sent within 1 s {on_time:6.2f}%  out of date when sent {stale:.3f}%  "
                  f"open orders {len(due):,}", flush=True)
            stats.clear()
            cutoff = time.time_ns() - 120e9                           # forget orders delivered 2+ minutes ago
            for o in [o for o, s in state.items() if s == len(STATES) - 1 and changed[o][-1] < cutoff]:
                del state[o], changed[o]

    await asyncio.gather(run(), receipts.consume_messages(on_receipt, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--orders-per-min", type=float, default=75_000,
                   help="new orders a minute; 75,000 is the reported peak 15 minutes into the 2026 Mega Blockbuster Sale")
    p.add_argument("--step-s", type=float, default=3, help="average seconds between an order's states (compressed)")
    p.add_argument("--sale", type=float, default=1.0, help="order-rate multiplier during the sale window")
    p.add_argument("--sale-at", type=float, default=0, help="seconds before the sale window starts")
    p.add_argument("--sale-for", type=float, default=0, help="sale window length in seconds")
    asyncio.run(main(p.parse_args()))
