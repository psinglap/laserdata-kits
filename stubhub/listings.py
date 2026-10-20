"""The source of truth: every seller listing on every event, and checkout's accept or reject.

Each listing change (tickets sold through another channel, a seller repricing, a seller pulling or relisting
tickets) goes to the listing-changes topic, keyed by event:listing, with a per-listing sequence number and the
resulting state (tickets left and price), never only a delta.

Buyers' seat picks arrive on the orders topic. A pick is confirmed if the listing still has the tickets, or
rejected as already sold. A rejection means a buyer chose seats the event page showed as available after they
were gone: the cost of a slow listing lane.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, LISTINGS, ORDERS, PARTITIONS, STATUS, STREAM, key, load, msg, part_of


async def main(a):
    async def own_client():                 # one connection per writer and reader, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await own_client()
    cfg = DirectProducerConfig(batch_length=500, linger_time=timedelta(microseconds=500))
    lane = await (await own_client()).producer(STREAM, LISTINGS, topic_partitions_count=PARTITIONS, mode=cfg)  # replicated by default
    status_p = await (await own_client()).producer(STREAM, STATUS, topic_partitions_count=8, mode=cfg)
    if await admin.get_topic(STREAM, ORDERS) is None:       # event_pages.py may not have created it yet
        try:
            await admin.create_topic(STREAM, ORDERS, 8)
        except RuntimeError:
            pass                                             # created by event_pages.py in the meantime
    orders = await (await own_client()).consumer_group(
        "checkout", STREAM, ORDERS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))

    keys = [key(e, l) for e in range(a.events) for l in range(a.listings)]
    by_event = [keys[e * a.listings:(e + 1) * a.listings] for e in range(a.events)]
    qty = {k: random.choice([0, 1, 2, 2, 4, 4, 6, 8]) for k in keys}   # tickets left on each listing
    price = {k: random.randint(40, 900) for k in keys}                   # USD per ticket, illustrative
    seq, stats = defaultdict(int), defaultdict(int)
    pending, statuses = [], []              # changes and order results queued for the next tick, sent in one batch

    def change(k, kind):
        seq[k] += 1
        return k, SendMessage(msg(k=k, seq=seq[k], qty=qty[k], price=price[k], kind=kind))

    async def send(changes):
        # One batch per partition, all sent at once: same per-listing order, about one round trip per tick.
        batches = defaultdict(list)
        for k, m in changes:
            batches[part_of(k)].append(m)
        await asyncio.gather(*(lane.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))

    async def on_order(m):
        o = load(m.payload())
        k, want = o["k"], o["qty"]
        if qty[k] >= want:
            qty[k] -= want
            stats["confirmed"] += 1
            if o["price"] != price[k]:
                stats["price_moved"] += 1   # the seller repriced between the page and the pick
            pending.append(change(k, "sold"))
            status = "confirmed"
        else:
            stats["rejected"] += 1          # shown as available, already sold
            status = "rejected_already_sold"
        statuses.append(SendMessage(msg(order=o["order"], k=k, status=status)))

    async def snapshot():
        # The starting state of every listing, like a CDC snapshot (seq 1, not timed as a live change).
        for i in range(0, len(keys), 5000):
            await send([change(k, "snapshot") for k in keys[i:i + 5000]])
        print(f"[listings] snapshot: {len(keys):,} listings across {a.events:,} events", flush=True)

    def one_change(k):
        r = random.random()
        if r < 0.45 and qty[k] > 0:                         # sold through another channel or another buyer
            qty[k] = max(0, qty[k] - random.choice([1, 2, 2, 4]))
            return change(k, "sold")
        if r < 0.75:                                         # seller reprices
            price[k] = max(10, round(price[k] * random.choice([0.9, 0.95, 1.05, 1.1])))
            return change(k, "repriced")
        if r < 0.85 and qty[k] > 0:                          # seller pulls the listing
            qty[k] = 0
            return change(k, "delisted")
        qty[k] += random.choice([1, 2, 2, 4])                # seller lists or adds tickets
        return change(k, "listed")

    async def churn():
        async with lane, status_p:
            await snapshot()
            gc.freeze()                     # keep start-up tables out of the collector's way
            t0 = due = time.monotonic()
            while True:
                t = time.monotonic() - t0
                onsale = a.onsale_at and a.onsale_at <= t < a.onsale_at + a.onsale_for
                n = max(1, int(a.rate * (a.onsale if onsale else 1) / 100))
                changes, pending[:] = list(pending), []
                for _ in range(n):
                    hot = onsale and random.random() < a.hot_share   # part of the onsale changes hit the one hot event
                    changes.append(one_change(random.choice(by_event[a.hot_event] if hot else keys)))
                out, statuses[:] = list(statuses), []
                sends = [send(changes)]
                if out:
                    sends.append(status_p.send_with_partitioning(out, Partitioning.balanced()))
                await asyncio.gather(*sends)
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            total = stats["confirmed"] + stats["rejected"]
            rate = 100 * stats["rejected"] / total if total else 0
            print(f"[listings] seat picks confirmed {stats['confirmed']:,}  rejected as already sold {stats['rejected']:,}  "
                  f"sold-before-checkout {rate:.3f}%  confirmed at a moved price {stats['price_moved']:,}  (this second)", flush=True)
            stats.clear()

    await asyncio.gather(churn(), orders.consume_messages(on_order, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--events", type=int, default=2000, help="events on sale")
    p.add_argument("--listings", type=int, default=250, help="seller listings per event")
    p.add_argument("--rate", type=int, default=2000, help="listing changes per second, all events")
    p.add_argument("--onsale", type=float, default=1.0, help="multiplier for changes during a big onsale")
    p.add_argument("--onsale-at", type=float, default=0, help="seconds after start")
    p.add_argument("--onsale-for", type=float, default=120)
    p.add_argument("--hot-event", type=int, default=7, help="the event that goes on sale")
    p.add_argument("--hot-share", type=float, default=0.2, help="share of onsale changes on the hot event")
    asyncio.run(main(p.parse_args()))
