"""Checkout and food preparation time (FPT) prediction, fed by the kitchen state lane.

Keeps a feature view of every kitchen from the kitchen-state topic: orders cooking, orders at
the counter, the five most recent running orders and the last five completed preparation
times. Customers check out at --orders-per-s; each checkout gets an FPT prediction from the
view as it is at that moment, and goes to the orders topic carrying the prediction and the
kitchen sequence the model saw, so kitchens.py can check it against the truth.

predict() is a stand-in for your model: it uses only the features the lane carries.

Prints event-to-feature-view latency every second (from the kitchen change being created to
this process having applied it) with PASS or FAIL against --budget-ms. On start it rebuilds
every kitchen by replaying the topic, so it holds no state; replayed events are not counted.
"""
import argparse
import asyncio
import os
import statistics
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, KITCHEN, ORDER_PARTITIONS, ORDERS, STREAM, Popularity, Window, load, msg, part


def predict(view) -> float:
    """Minutes from accept to Food Order Ready, from the kitchen as the model sees it."""
    if view is None:
        return 15.0
    base = statistics.median(view["done"]) if view["done"] else 15.0
    return round(base + 1.5 * max(0, view["cooking"] - 2), 1)


async def connected(url):
    # One connection per reader and writer, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def main(a):
    client = await connected(a.url)
    while not all([await client.get_topic(STREAM, t) for t in (KITCHEN, ORDERS)]):
        print("[fpt] waiting for kitchens.py to create the topics…", flush=True)
        await asyncio.sleep(1)
    started_ns = time.time_ns()
    lane = await (await connected(a.url)).consumer_group(
        f"fpt-features-{os.getpid()}", STREAM, KITCHEN, polling_strategy=PollingStrategy.First(), batch_length=5000,
        auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))
    out = await client.producer(STREAM, ORDERS, topic_partitions_count=ORDER_PARTITIONS,
                                mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200)))
    pop = Popularity(a.restaurants, a.skew)
    view, lat, stats = {}, Window(), defaultdict(int)

    async def on_kitchen(m):
        e = load(m.payload())
        v = view.get(e["r"])
        if v is not None and e["seq"] <= v["seq"]:
            return                          # older or repeated change: the newest state wins
        view[e["r"]] = e
        if e["seq"] > 1 and e["ns"] >= started_ns:      # seq 1 is the snapshot; replayed history is a rebuild
            lat.add((time.time_ns() - e["ns"]) / 1e6)

    async def checkout():
        n, owed, t0 = 0, 0.0, time.monotonic()
        due = t0
        async with out:
            while True:
                t = time.monotonic() - t0
                rush = a.rush if a.rush_at and a.rush_at <= t < a.rush_at + a.rush_for else 1
                owed += a.orders_per_s * rush / 100
                batches = defaultdict(list)
                while owed >= 1:
                    owed -= 1
                    n += 1
                    r = pop.pick()
                    v = view.get(r)
                    batches[part(r, ORDER_PARTITIONS)].append(SendMessage(msg(
                        order=f"Z{os.getpid()}-{n}", r=r, seen=v["seq"] if v else 0, fpt=predict(v), pred_ns=time.time_ns())))
                    stats["predictions"] += 1
                await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in batches.items()))
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[fpt] {r['n']:>6,} kitchen events/s  event-to-feature-view p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  FPT predictions {stats['predictions']:,}  "
                  f"kitchens in view {len(view):,}", flush=True)
            stats.clear()

    await asyncio.gather(lane.consume_messages(on_kitchen, asyncio.Event()), checkout(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--restaurants", type=int, default=150_000, help="must match kitchens.py")
    p.add_argument("--skew", type=float, default=0.6, help="must match kitchens.py")
    p.add_argument("--orders-per-s", type=float, default=140, help="checkouts per second (NYE 2023 peak: 8,400 a minute)")
    p.add_argument("--rush", type=float, default=1.0, help="multiplier for checkouts during the rush")
    p.add_argument("--rush-at", type=float, default=0)
    p.add_argument("--rush-for", type=float, default=120)
    p.add_argument("--budget-ms", type=int, default=100)
    asyncio.run(main(p.parse_args()))
