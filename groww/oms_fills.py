"""OMS fills: every executed trade, published as the user's resulting position and an order update.

Plays the order broadcast leg. When the exchange reports a fill, OMS knows the user's new
position in that instrument. This script publishes it twice, keyed by user ID:
  * position-updates: the lane. The resulting net quantity and average price (never only a
    delta), with a per-position sequence number, for the Holdings service;
  * order-updates: the update the app receives. The app refreshes Holdings as soon as it
    arrives, so holdings.py checks that refresh against the position it holds.

Market open is a burst: --open-burst times the fill rate for --open-for seconds, starting
--open-at seconds in (9:15 IST in real life).
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, ORDER_UPDATES, PARTITIONS, POSITIONS, STREAM, ensure_topics, instrument, msg, part, user_id


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), POSITIONS, ORDER_UPDATES)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    pos_p = await (await own_client()).producer(STREAM, POSITIONS, topic_partitions_count=PARTITIONS[POSITIONS], mode=cfg)
    ord_p = await (await own_client()).producer(STREAM, ORDER_UPDATES, topic_partitions_count=PARTITIONS[ORDER_UPDATES], mode=cfg)
    qty, avg, seq = defaultdict(int), {}, defaultdict(int)
    price = [round(random.uniform(20, 4000), 2) for _ in range(a.instruments)]
    stats = defaultdict(int)
    n_orders = 0
    gc.freeze()                             # keep start-up objects out of the collector's way

    def fill():
        nonlocal n_orders
        n_orders += 1
        u = user_id(random.randrange(a.users))
        i = min(int(random.paretovariate(1.2)) - 1, a.instruments - 1)   # a few instruments take most trades
        k = f"{u}:{instrument(i)}"
        side = 1 if qty[k] <= 0 or random.random() < 0.55 else -1       # buy, or sell part of what is held
        q = random.choice((1, 1, 2, 5, 10, 25, 50)) if side > 0 else random.randint(1, qty[k])
        px = price[i] = round(price[i] * random.uniform(0.999, 1.001), 2)
        if side > 0:
            avg[k] = round((avg.get(k, 0) * qty[k] + px * q) / (qty[k] + q), 2)
        qty[k] += side * q
        seq[k] += 1
        now = time.time_ns()
        order = f"GRW{n_orders:012d}"
        stats["fills"] += 1
        return u, (SendMessage(msg(k=k, u=u, seq=seq[k], qty=qty[k], avg=avg.get(k, 0), order=order, ns=now)),
                   SendMessage(msg(k=k, u=u, seq=seq[k], order=order, status="EXECUTED", side="BUY" if side > 0 else "SELL",
                                   q=q, px=px, ns=now)))

    async def send(producer, batch, parts):
        # One batch per partition, all sent at once: per-user order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for u, m in batch:
            groups[part(u, parts)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def run():
        t0 = due = time.monotonic()
        owed = 0.0
        async with pos_p, ord_p:
            while True:
                t = time.monotonic() - t0
                x = a.open_burst if a.open_at <= t < a.open_at + a.open_for else 1
                owed += a.fills_per_s * x / 100
                made = [fill() for _ in range(int(owed))]
                owed -= int(owed)
                if made:
                    await asyncio.gather(send(pos_p, [(u, m[0]) for u, m in made], PARTITIONS[POSITIONS]),
                                         send(ord_p, [(u, m[1]) for u, m in made], PARTITIONS[ORDER_UPDATES]))
                stats["burst"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "market open" if stats["burst"] else "steady"
            print(f"[oms] {stats['fills']:>7,} fills/s ({phase})  positions held {len(qty):,}", flush=True)
            stats["fills"] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--users", type=int, default=1_000_000, help="users trading today")
    p.add_argument("--instruments", type=int, default=2_000)
    p.add_argument("--fills-per-s", type=float, default=3_000)
    p.add_argument("--open-burst", type=float, default=5, help="fill-rate multiplier at market open")
    p.add_argument("--open-at", type=float, default=0, help="seconds after start")
    p.add_argument("--open-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
