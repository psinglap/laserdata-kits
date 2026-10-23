"""Event pages and buyers, reading the listing lane.

Keeps a live view of every event (which listings still have tickets, and at what price) from the
listing-changes topic, and serves buyers from it: each visit opens an event page, picks a listing the page shows
as available, and sends the seat pick to checkout (listings.py), which confirms it or rejects it as already sold.

For comparison it also keeps the same view held --lag seconds behind (what a cached or slow-fed page would show)
and counts, on every page served, the listings that view would still show although they are already sold.

Prints change-to-page latency every second (p50, p99, p99.9) against the budget, and that comparison.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, LISTINGS, ORDERS, STREAM, Window, load, msg, read_partitions


async def main(a):
    async def own_client():                 # one connection per reader and writer, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await own_client()
    while not await admin.get_topic(STREAM, LISTINGS):
        print(f"[{a.name}] waiting for listings.py to create the topic…", flush=True)
        await asyncio.sleep(1)
    lane = await own_client()               # direct partition reads, rebuilt from the first message
    lane_parts = range((await admin.get_topic(STREAM, LISTINGS)).partitions_count)
    orders = await (await own_client()).producer(STREAM, ORDERS, topic_partitions_count=8)

    seq, qty, price = {}, {}, {}
    live = defaultdict(set)                 # event -> listings with tickets left, as of the newest change
    behind = defaultdict(set)               # the same view, --lag seconds behind
    delayed = deque()                       # changes waiting to reach the lagged view
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays changes made before it joined

    async def on_change(m):
        c = load(m.payload())
        k = c["k"]
        if c["seq"] <= seq.get(k, 0):
            return                          # older or repeated change: the newest state wins
        seq[k] = c["seq"]
        if c["seq"] > 1:                    # seq 1 is the startup snapshot, not a live change
            age = (time.time_ns() - c["ns"]) / 1e6
            if caught_up[0]:
                lat.add(age)
            else:                           # still replaying: track the oldest change seen this second
                stats["replayed"] += 1
                stats["max_age"] = max(stats["max_age"], age)
        event = k.split(":")[0]
        qty[k], price[k] = c["qty"], c["price"]
        (live[event].add if c["qty"] > 0 else live[event].discard)(k)
        delayed.append((c["ns"], event, k, c["qty"]))

    async def age_lagged_view():
        while True:
            await asyncio.sleep(0.05)
            cutoff = time.time_ns() - int(a.lag * 1e9)
            while delayed and delayed[0][0] <= cutoff:
                _, event, k, q = delayed.popleft()
                (behind[event].add if q > 0 else behind[event].discard)(k)

    async def buyers():
        n, per_tick, due = 0, max(1, int(a.picks_per_s / 100)), time.monotonic()
        events = None
        async with orders:
            while True:
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))
                if not caught_up[0] or not live:
                    continue
                events = events or sorted(live)
                picks = []
                for _ in range(per_tick):
                    hot = f"E{a.hot_event:05d}"
                    event = hot if random.random() < a.hot_share and hot in live else random.choice(events)
                    shown = live[event]
                    stale = behind[event] - shown     # a page --lag s behind still lists these, already sold
                    stats["page_listings"] += len(behind[event])
                    stats["stale"] += len(stale)
                    if not shown:
                        continue
                    k = random.choice(tuple(shown))
                    n += 1
                    picks.append(SendMessage(msg(order=f"{a.name}-{n}", k=k, qty=random.randint(1, min(4, qty[k])),
                                                 price=price[k])))
                stats["picks"] += len(picks)
                if picks:
                    await orders.send_with_partitioning(picks, Partitioning.balanced())

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of changes arrived inside the budget; partitions interleave,
                # so one fresh change alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                    gc.freeze()             # keep the replayed view out of the collector's way
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier changes this second", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            stale = 100 * stats["stale"] / stats["page_listings"] if stats["page_listings"] else 0
            print(f"[{a.name}] {r['n']:>7,} changes/s  change-to-page p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  seat picks {stats['picks']:,}  "
                  f"a page {a.lag:g}s behind would list {stats['stale']:,} already-sold listings ({stale:.2f}%)", flush=True)
            stats.clear()

    await asyncio.gather(read_partitions(lane, LISTINGS, lane_parts, f"{a.name}-{os.getpid()}", on_change, replay=True,
                                         poll_ms=0.5), age_lagged_view(), buyers(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="pages")
    p.add_argument("--picks-per-s", type=float, default=300, help="buyers picking seats per second")
    p.add_argument("--hot-event", type=int, default=7, help="the event most buyers are on during an onsale")
    p.add_argument("--hot-share", type=float, default=0.3, help="share of buyers on the hot event")
    p.add_argument("--lag", type=float, default=2.0, help="seconds behind, for the comparison view")
    p.add_argument("--budget-ms", type=int, default=100)
    asyncio.run(main(p.parse_args()))
