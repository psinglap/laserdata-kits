"""The vehicle context agents read, fed by the lane, and agents answering shoppers from it.

Keeps a live view of every rooftop (which vehicles are In Stock, at what price and miles) from the vehicle-changes
topic. Each answer is a shopper's lead on one rooftop: the agent (Salesperson AI, or T1 Pro for staff) offers up
to three In Stock vehicles with their prices from the live view, and books a test drive on one of them through the
DMS (dms_inventory.py), which confirms it or declines it because the vehicle is no longer available.

For comparison it also keeps what a context cache with a --ttl second TTL per rooftop would hold, and, on every
answer, judges the three vehicles that cache would have offered against the same live state the agent used:
offered although no longer In Stock, or offered at a price that has since changed.

Prints change-to-context latency every second (p50, p99, p99.9) against the budget, and that comparison.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import AVAILABLE, BOOKINGS, DEFAULT_URL, GONE, STREAM, VEHICLES, Window, load, msg, read_partitions, rooftop_of


async def main(a):
    async def own_client():                 # one connection per reader and writer, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await own_client()
    while not await admin.get_topic(STREAM, VEHICLES):
        print(f"[{a.name}] waiting for dms_inventory.py to create the topic…", flush=True)
        await asyncio.sleep(1)
    lane = await own_client()               # direct partition reads, rebuilt from the first message
    lane_parts = range((await admin.get_topic(STREAM, VEHICLES)).partitions_count)
    bookings = await (await own_client()).producer(STREAM, BOOKINGS, topic_partitions_count=8)

    seq, price, miles = {}, {}, {}
    in_stock = defaultdict(set)             # rooftop -> vehicles In Stock, as of the newest change
    cache = {}                              # rooftop -> (refreshed at, {vehicle: price}): a TTL cache of the same view
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays changes made before it joined

    async def on_change(m):
        c = load(m.payload())
        k = c["k"]
        if c["seq"] <= seq.get(k, 0):
            return                          # older or repeated change: the newest state wins
        seq[k] = c["seq"]
        if not c.get("snap"):               # the startup snapshot is not a live change
            age = (time.time_ns() - c["ns"]) / 1e6
            if caught_up[0]:
                lat.add(age)
            else:                           # still replaying: track the oldest change seen this second
                stats["replayed"] += 1
                stats["max_age"] = max(stats["max_age"], age)
        r = rooftop_of(k)
        if c["status"] == AVAILABLE:
            in_stock[r].add(k)
            price[k], miles[k] = c["price"], c["miles"]
        else:
            in_stock[r].discard(k)
            if c["status"] in GONE:         # left the lot: drop it from the context, keep only its sequence
                price.pop(k, None)
                miles.pop(k, None)
            else:
                price[k], miles[k] = c["price"], c["miles"]

    def cached_view(r, now):
        hit = cache.get(r)
        if hit is None:                     # first read: as if refreshed at a random point in the last TTL
            hit = cache[r] = (now - random.random() * a.ttl, {k: price[k] for k in in_stock[r]})
        elif now - hit[0] >= a.ttl:
            hit = cache[r] = (now, {k: price[k] for k in in_stock[r]})
        return hit[1]

    async def agents():
        n, per_tick, due = 0, max(1, int(a.answers_per_s / 100)), time.monotonic()
        rooftops = None
        async with bookings:
            while True:
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))
                if not caught_up[0] or not in_stock:
                    continue
                rooftops = rooftops or sorted(in_stock)
                now, out = time.monotonic(), []
                for _ in range(per_tick):
                    r = random.choice(rooftops)
                    live = in_stock[r]
                    if not live:
                        continue
                    # The same lead answered from the TTL cache, judged against the same live state.
                    old = cached_view(r, now)
                    offer = random.sample(tuple(old), min(3, len(old))) if old else []
                    gone = sum(1 for k in offer if k not in live)
                    moved = sum(1 for k in offer if k in live and old[k] != price[k])
                    stats["answers"] += 1
                    stats["ttl_wrong"] += 1 if gone or moved else 0
                    stats["ttl_gone"] += gone
                    stats["ttl_moved"] += moved
                    # The agent's real answer, from the live view, and a test drive on one of the offered vehicles.
                    k = random.choice(random.sample(tuple(live), min(3, len(live))))
                    n += 1
                    out.append(SendMessage(msg(booking=f"{a.name}-{n}", k=k, price=price[k], miles=miles[k])))
                stats["bookings"] += len(out)
                if out:
                    await bookings.send_with_partitioning(out, Partitioning.balanced())

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
            wrong = 100 * stats["ttl_wrong"] / stats["answers"] if stats["answers"] else 0
            print(f"[{a.name}] {r['n']:>6,} changes/s  change-to-context p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  answers {stats['answers']:,}  "
                  f"test drives {stats['bookings']:,}  a {a.ttl:g}s TTL cache would have offered {stats['ttl_gone']:,} "
                  f"vehicles no longer in stock and {stats['ttl_moved']:,} old prices ({wrong:.1f}% of answers)", flush=True)
            stats.clear()

    await asyncio.gather(read_partitions(lane, VEHICLES, lane_parts, f"{a.name}-{os.getpid()}", on_change, replay=True,
                                         poll_ms=0.5), agents(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="context")
    p.add_argument("--answers-per-s", type=float, default=100, help="agent answers on vehicles per second, all rooftops")
    p.add_argument("--ttl", type=float, default=30.0, help="seconds, for the comparison cache (illustrative)")
    p.add_argument("--budget-ms", type=int, default=100)
    asyncio.run(main(p.parse_args()))
