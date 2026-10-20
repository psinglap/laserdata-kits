"""The Holdings service: positions kept current from the lane, and the app's refresh checked against them.

Two readers, each on its own connection and consumer group:
  * position-updates (the lane): applies each fill's resulting position to the user's
    holdings view, ignoring any older or repeated sequence, and invalidates the user's
    ETag so the next Holdings request is rebuilt. Fill-to-holdings latency is timed here.
  * order-updates: plays the app. When an order update arrives, the app refreshes Holdings
    right away, which Groww measured as about 500 ms after the fill. At that moment the
    script checks the view: does Holdings already show this fill?

Prints fill-to-holdings p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and
the business number: the share of post-trade refreshes that did not show the new fill.
"""
import argparse
import asyncio
import gc
import heapq
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import DEFAULT_URL, ORDER_UPDATES, POSITIONS, REFRESH_MS, STREAM, Window, ensure_topics, load


async def main(a):
    async def own_client():                 # one connection per reader, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), POSITIONS, ORDER_UPDATES)

    async def group(topic):
        return await (await own_client()).consumer_group(
            f"{a.name}-{topic}-{os.getpid()}", STREAM, topic, polling_strategy=PollingStrategy.First(), batch_length=5000,
            auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))

    positions, orders = await group(POSITIONS), await group(ORDER_UPDATES)
    view = {}                               # user:instrument -> (seq, net qty, avg price): the holdings view
    etag_dirty = set()                      # users whose Holdings ETag was invalidated this second
    refreshes = []                          # (when the app refreshes, user:instrument, seq of the fill)
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays fills made before it joined
    gc.freeze()

    async def on_position(m):
        p = load(m.payload())
        k = p["k"]
        if p["seq"] <= view.get(k, (0,))[0]:
            stats["dropped"] += 1           # older or repeated fill: the newest position wins
            return
        view[k] = (p["seq"], p["qty"], p["avg"])
        etag_dirty.add(p["u"])              # the next Holdings request rebuilds instead of returning 304
        age = (time.time_ns() - p["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest fill seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_order_update(m):
        o = load(m.payload())
        if not caught_up[0] or time.time_ns() - o["ns"] > 5e9:
            return                          # replayed history, not a refresh happening now
        heapq.heappush(refreshes, (o["ns"] + a.refresh_ms * 1_000_000, o["k"], o["seq"]))

    async def app_refresh():
        while True:
            await asyncio.sleep(0.005)
            now = time.time_ns()
            while refreshes and refreshes[0][0] <= now:
                _, k, s = heapq.heappop(refreshes)
                stats["checked"] += 1
                if view.get(k, (0,))[0] < s:
                    stats["missed"] += 1    # the app refreshed and Holdings did not show the new fill yet

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of fills arrived inside the budget; partitions interleave,
                # so one fresh fill alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier fills this second", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            missed = 100 * stats["missed"] / stats["checked"] if stats["checked"] else 0
            print(f"[{a.name}] {r['n']:>7,} position updates/s  fill-to-holdings p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  ETags invalidated {len(etag_dirty):,}", flush=True)
            print(f"[app] post-trade refreshes {stats['checked']:,}  missing the new fill {missed:.3f}% "
                  f"(Holdings refreshed {a.refresh_ms} ms after the fill)", flush=True)
            etag_dirty.clear()
            stats.clear()

    stop = asyncio.Event()
    await asyncio.gather(positions.consume_messages(on_position, stop), orders.consume_messages(on_order_update, stop),
                         app_refresh(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="holdings")
    p.add_argument("--budget-ms", type=int, default=100, help="fill-to-holdings p99 budget for the lane")
    p.add_argument("--refresh-ms", type=int, default=REFRESH_MS, help="when the app refreshes Holdings, after the fill")
    asyncio.run(main(p.parse_args()))
