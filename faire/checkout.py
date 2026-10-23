"""The multi-brand checkout's live stock view, fed by the lane, and retailers checking out Fulfilled by Faire carts on it.

Keeps a live view of every FBF SKU (units available, case pack) from the fbf-stock topic. Each checkout is one
retailer's cart across several FBF brands (one minimum, one shipment): one to three SKUs per brand, each in whole
case packs. Checkout trims any line to the cases the live view says are available, then places the order with the
order service (warehouse.py), which reserves each line or marks it short.

For comparison it also keeps what a periodic inventory sync every --sync-every seconds would hold per brand, and,
on every checkout, judges the lines a cart built on that synced view would have accepted against the same live
state the real checkout used: lines accepted that the warehouse can no longer fill.

Prints change-to-checkout latency every second (p50, p99, p99.9) against the budget, and that comparison.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, ORDERS, STOCK, STREAM, Window, brand_of, load, msg, read_partitions


async def main(a):
    async def own_client():                 # one connection per reader and writer, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await own_client()
    while not await admin.get_topic(STREAM, STOCK):
        print(f"[{a.name}] waiting for warehouse.py to create the topic…", flush=True)
        await asyncio.sleep(1)
    lane = await own_client()               # direct partition reads, rebuilt from the first message
    lane_parts = range((await admin.get_topic(STREAM, STOCK)).partitions_count)
    orders = await (await own_client()).producer(STREAM, ORDERS, topic_partitions_count=8)

    seq, avail, case = {}, {}, {}
    in_stock = defaultdict(set)             # brand -> SKUs with at least one case available, as of the newest change
    synced = {}                             # brand -> (synced at, {SKU: available}): a periodic sync of the same view
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays changes made before it joined

    async def on_change(m):
        c = load(m.payload())
        k = c["k"]
        if c["seq"] <= seq.get(k, 0):
            return                          # older or repeated change: the newest levels win
        seq[k] = c["seq"]
        if not c.get("snap"):               # the startup snapshot is not a live change
            age = (time.time_ns() - c["ns"]) / 1e6
            if caught_up[0]:
                lat.add(age)
            else:                           # still replaying: track the oldest change seen this second
                stats["replayed"] += 1
                stats["max_age"] = max(stats["max_age"], age)
        avail[k], case[k] = c["avail"], c["case"]
        (in_stock[brand_of(k)].add if c["avail"] >= c["case"] else in_stock[brand_of(k)].discard)(k)

    def synced_view(b, now):
        hit = synced.get(b)
        if hit is None:                     # first read: as if synced at a random point in the last interval
            hit = synced[b] = (now - random.random() * a.sync_every, {k: avail[k] for k in in_stock[b]})
        elif now - hit[0] >= a.sync_every:
            hit = synced[b] = (now, {k: avail[k] for k in in_stock[b]})
        return hit[1]

    async def checkouts():
        n, owed, due = 0, 0.0, time.monotonic()
        brands = None
        async with orders:
            while True:
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))
                if not caught_up[0] or not in_stock:
                    continue
                brands = brands or sorted(in_stock)
                now, out = time.monotonic(), []
                owed += a.carts_per_s / 100         # carts this 10 ms tick owes, fractions carried to the next
                per_tick = int(owed)
                owed -= per_tick
                for _ in range(per_tick):
                    cart_brands = random.sample(brands, min(len(brands), random.randint(2, 5)))
                    lines, wrong = [], 0
                    for b in cart_brands:
                        # The same retailer's lines for this brand built on the synced view, judged against the
                        # same live state the real checkout uses below.
                        old = synced_view(b, now)
                        for k in random.sample(tuple(old), min(len(old), random.randint(1, 3))) if old else ():
                            q = case[k] * random.randint(1, 2)
                            if old[k] >= q:
                                stats["sync_lines"] += 1
                                if avail.get(k, 0) < q:
                                    wrong += 1          # the sync says yes, the warehouse can't fill it
                        # The real cart, built and checked on the live view.
                        live = in_stock[b]
                        for k in random.sample(tuple(live), min(len(live), random.randint(1, 3))) if live else ():
                            q = case[k] * random.randint(1, 2)
                            if q > avail[k]:
                                q = case[k] * (avail[k] // case[k])   # trimmed to the cases still available
                                stats["trimmed"] += 1
                            if q:
                                lines.append({"k": k, "qty": q})
                    stats["carts"] += 1
                    stats["sync_short"] += wrong
                    stats["sync_carts"] += 1 if wrong else 0
                    if lines:
                        n += 1
                        stats["lines"] += len(lines)
                        out.append(SendMessage(msg(order=f"{a.name}-{n}", lines=lines)))
                stats["orders"] += len(out)
                if out:
                    await orders.send_with_partitioning(out, Partitioning.balanced())

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
            share = 100 * stats["sync_carts"] / stats["carts"] if stats["carts"] else 0
            print(f"[{a.name}] {r['n']:>6,} changes/s  change-to-checkout p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  carts {stats['carts']:,}  orders {stats['orders']:,}  "
                  f"lines {stats['lines']:,}  a {a.sync_every:g}s inventory sync would have accepted {stats['sync_short']:,} "
                  f"lines the warehouse can't fill ({share:.1f}% of carts)", flush=True)
            stats.clear()

    await asyncio.gather(read_partitions(lane, STOCK, lane_parts, f"{a.name}-{os.getpid()}", on_change, replay=True,
                                         poll_ms=0.5), checkouts(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="checkout")
    p.add_argument("--carts-per-s", type=float, default=20, help="multi-brand FBF checkouts per second")
    p.add_argument("--sync-every", type=float, default=300.0, help="seconds, for the comparison sync (illustrative)")
    p.add_argument("--budget-ms", type=int, default=100)
    asyncio.run(main(p.parse_args()))
