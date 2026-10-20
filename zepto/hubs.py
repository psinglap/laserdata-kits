"""The source of truth: stock and price for every SKU in every hub, and order acceptance.

Inventory and pricing changes go to their own P0 topics, keyed by hub:sku, each with a
per-key sequence number and the resulting value (level or price), never only a delta.
Orders arrive on the orders topic and are accepted or rejected against true stock.
A rejection means a shopper was shown an item, after re-validation, that the hub no
longer had: the cost of a slow P0 lane.
"""
import argparse
import asyncio
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, INVENTORY, ORDERS, PRICING, STATUS, STREAM, key, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    cfg = DirectProducerConfig(batch_length=500, linger_time=timedelta(microseconds=500))
    durable = dict(topic_partitions_count=a.partitions, mode=cfg)   # new topics default to replicated durability
    inv_p = await client.producer(STREAM, INVENTORY, **durable)
    price_p = await client.producer(STREAM, PRICING, **durable)
    status_p = await client.producer(STREAM, STATUS, topic_partitions_count=8, mode=cfg)
    if await client.get_topic(STREAM, ORDERS) is None:      # revalidate.py may not have created it yet
        try:
            await client.create_topic(STREAM, ORDERS, 8)
        except RuntimeError:
            pass                                             # created by revalidate.py in the meantime
    orders = await client.consumer_group("hubs", STREAM, ORDERS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
                                         auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))
    keys = [key(h, s) for h in range(a.hubs) for s in range(a.skus)]
    level = {k: random.randint(0, 6) for k in keys}          # a dark store holds a few units of each SKU
    price = {k: random.randint(15, 400) for k in keys}       # rupees
    inv_seq, price_seq, stats = defaultdict(int), defaultdict(int), defaultdict(int)

    def inv_change(k):
        inv_seq[k] += 1
        return SendMessage(msg(k=k, seq=inv_seq[k], level=level[k]))

    def price_change(k):
        price_seq[k] += 1
        return SendMessage(msg(k=k, seq=price_seq[k], price=price[k]))

    async def on_order(m):
        o = load(m.payload())
        k = o["k"]
        if level[k] > 0:
            level[k] -= 1
            stats["accepted"] += 1
            await inv_p.send_with_partitioning([inv_change(k)], Partitioning.messages_key(k))
            status = "confirmed"
        else:
            stats["rejected"] += 1      # shown as in stock after re-validation, already gone
            status = "rejected_sold_out"
        await status_p.send_with_partitioning([SendMessage(msg(order=o["order"], status=status))], Partitioning.messages_key(o["order"]))

    async def snapshot():
        # Starting level and price of every SKU, like a CDC snapshot. Batches are spread across
        # partitions; the per-key sequence keeps it safe, since a reader keeps only the newest.
        for i in range(0, len(keys), 1000):
            await inv_p.send_with_partitioning([inv_change(k) for k in keys[i:i + 1000]], Partitioning.balanced())
            await price_p.send_with_partitioning([price_change(k) for k in keys[i:i + 1000]], Partitioning.balanced())
        print(f"[hubs] snapshot: {len(keys):,} SKUs across {a.hubs} hubs", flush=True)

    async def churn():
        async with inv_p, price_p, status_p:
            await snapshot()
            t0, due = time.monotonic(), time.monotonic()
            while True:
                t = time.monotonic() - t0
                peak = a.festive if a.festive_at and a.festive_at <= t < a.festive_at + a.festive_for else 1
                inv, prices = {}, {}
                for _ in range(max(1, int(a.rate * peak / 100))):
                    k = random.choice(keys)
                    if random.random() < 0.2:                       # price or offer change
                        price[k] = max(5, round(price[k] * random.choice([0.9, 0.95, 1.05])))
                        prices.setdefault(k, []).append(price_change(k))
                    else:
                        if random.random() < 0.75 and level[k] > 0:
                            level[k] -= 1                           # picked for another order
                        else:
                            level[k] += random.randint(1, 6)        # restock from the mother hub
                        inv.setdefault(k, []).append(inv_change(k))
                for k, ms in inv.items():
                    await inv_p.send_with_partitioning(ms, Partitioning.messages_key(k))
                for k, ms in prices.items():
                    await price_p.send_with_partitioning(ms, Partitioning.messages_key(k))
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            total = stats["accepted"] + stats["rejected"]
            rate = 100 * stats["rejected"] / total if total else 0
            print(f"[hubs] orders accepted {stats['accepted']:,}  rejected as sold out {stats['rejected']:,}  "
                  f"shown-but-gone {rate:.3f}%", flush=True)

    await asyncio.gather(churn(), orders.consume_messages(on_order, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--hubs", type=int, default=300)
    p.add_argument("--skus", type=int, default=2000, help="SKUs per hub")
    p.add_argument("--rate", type=int, default=5000, help="inventory and price changes per second, all hubs")
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--festive", type=float, default=1.0, help="multiplier for changes during the festive peak")
    p.add_argument("--festive-at", type=float, default=0)
    p.add_argument("--festive-for", type=float, default=120)
    asyncio.run(main(p.parse_args()))
