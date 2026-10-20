"""Search results, re-validated against the live P0 lane before the shopper sees them.

Two views of every hub:
  * the search index, rebuilt from the live view every --refresh seconds (10 s, like a
    segment-replication refresh), which retrieval reads;
  * the live view, kept current from the inventory and pricing topics, which
    re-validation reads.

Each search picks a hub, takes candidates the index says are in stock, drops any the live
view says are gone, and orders one survivor. Prints event-to-live-view latency every
second, how many index results re-validation removed, and (in hubs.py) how many orders
were still rejected because the live view itself was behind.
"""
import argparse
import asyncio
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, INVENTORY, ORDERS, PRICING, STREAM, Window, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    while not all([await client.get_topic(STREAM, t) for t in (INVENTORY, PRICING)]):
        print(f"[{a.name}] waiting for hubs.py to create the topics…", flush=True)
        await asyncio.sleep(1)

    async def own_client():                 # one connection per reader and writer, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    async def group(topic):
        return await (await own_client()).consumer_group(
            f"{a.name}-{topic}-{os.getpid()}", STREAM, topic, polling_strategy=PollingStrategy.First(), batch_length=5000,
            auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))

    inv, prices = await group(INVENTORY), await group(PRICING)
    orders = await (await own_client()).producer(STREAM, ORDERS, topic_partitions_count=8)
    seq, live, price, index = {}, defaultdict(set), {}, {}
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays changes made before it joined

    def newer(c, tag):
        k = (tag, c["k"])
        if c["seq"] <= seq.get(k, 0):
            return False                    # older or repeated change: the newest value wins
        seq[k] = c["seq"]
        if c["seq"] > 1:                    # seq 1 is the startup snapshot, not a live change
            age = (time.time_ns() - c["ns"]) / 1e6
            if caught_up[0]:
                lat.add(age)
            else:                           # still replaying: track the oldest change seen this second
                stats["replayed"] += 1
                stats["max_age"] = max(stats["max_age"], age)
        return True

    async def on_inventory(m):
        c = load(m.payload())
        if newer(c, "i"):
            hub, sku = c["k"].split(":")
            (live[hub].add if c["level"] > 0 else live[hub].discard)(sku)

    async def on_price(m):
        c = load(m.payload())
        if newer(c, "p"):
            price[c["k"]] = c["price"]

    async def refresh_index():
        while True:
            await asyncio.sleep(a.refresh)
            index.clear()
            index.update({hub: tuple(skus) for hub, skus in live.items() if skus})

    async def search():
        n = 0
        async with orders:
            while True:
                await asyncio.sleep(1 / a.searches_per_s)
                if not index:
                    continue
                hub = random.choice(tuple(index))
                candidates = random.sample(index[hub], min(10, len(index[hub])))
                shown = [s for s in candidates if s in live[hub]]          # re-validation
                stats["candidates"] += len(candidates)
                stats["removed"] += len(candidates) - len(shown)
                if shown:
                    n += 1
                    k = f"{hub}:{random.choice(shown)}"
                    await orders.send_with_partitioning([SendMessage(msg(order=f"{a.name}-{n}", k=k, price=price.get(k)))],
                                                        Partitioning.messages_key(k))

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of changes arrived inside the budget; partitions interleave,
                # so one fresh change alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier changes this second", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            removed = 100 * stats["removed"] / stats["candidates"] if stats["candidates"] else 0
            print(f"[{a.name}] {r['n']:>7,} changes/s  event-to-live-view p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"index results already gone, caught by re-validation {removed:5.2f}%", flush=True)
            stats.clear()

    stop = asyncio.Event()
    await asyncio.gather(inv.consume_messages(on_inventory, stop), prices.consume_messages(on_price, stop),
                         refresh_index(), search(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="revalidate")
    p.add_argument("--searches-per-s", type=float, default=200)
    p.add_argument("--refresh", type=float, default=10, help="seconds between search index refreshes")
    p.add_argument("--budget-ms", type=int, default=100)
    asyncio.run(main(p.parse_args()))
