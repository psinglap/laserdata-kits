"""The Instacart storefront: shoppers view products, open item pages and add to cart, and ads carousels load.

Plays the app during a busy shopping window. Each shopper action is sent, keyed by user, to:
  * session-actions: the lane. The action kind (product view, item page visit, cart add, the session events
    Instacart's ads retrieval post says the model reads), the product's token, the retailer type, the
    shopper's session and a sequence number that only grows for each user, for the session sequence service;
  * carousel-requests: a short time after an action, the shopper's next ads carousel (retailer home page,
    items to add next on an item page, or pre-checkout after a cart add). It carries the sequence of the
    shopper's last action, which the session sequence must already hold, its kind and its product token.

Shoppers build a cart in one aisle for a while and then move on (--switch-share): a shopper who was adding
ground beef and buns turns to laundry, and the next carousel should follow. --gap-min/--gap-max set the time
from an action to the next carousel request; --surge multiplies the rate for --surge-for seconds (the
evening before a holiday, when everyone is filling a cart at once).
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (ACTIONS, AISLES, CAROUSELS, DEFAULT_URL, KINDS, PARTITIONS, RETAILER_TYPES, STREAM,
                    ensure_topics, msg, part, sid, user_id)

KIND_MIX = [0.45, 0.25, 0.30]            # product view, item page visit, cart add (illustrative)
SURFACE = {"product_view": "retailer home", "item_page_visit": "items to add next", "add_to_cart": "pre-checkout"}


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ACTIONS, CAROUSELS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    act_p = await (await own_client()).producer(STREAM, ACTIONS, topic_partitions_count=PARTITIONS[ACTIONS], mode=cfg)
    car_p = await (await own_client()).producer(STREAM, CAROUSELS, topic_partitions_count=PARTITIONS[CAROUSELS], mode=cfg)
    # Action sequence: one counter for all users, started from the clock, so each user's sequence only grows,
    # even across restarts of this script, without keeping a counter per user.
    next_seq = iter(range(time.time_ns(), 1 << 63))
    run = os.urandom(3).hex()               # sessions from this run of the app
    rnd = random.Random()
    shoppers = {}                           # user -> [session, retailer type, aisle]
    pending = []                            # (when, user, needs, kind, token): the next carousel request
    stats = defaultdict(int)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def action(user, now_ns):
        s = shoppers.get(user)
        if s is None:
            s = shoppers[user] = [f"{run}-{user}", rnd.choice(RETAILER_TYPES), rnd.randrange(len(AISLES))]
        if rnd.random() < a.switch_share:
            s[2] = rnd.randrange(len(AISLES))   # intent shifts mid-cart: a new aisle
            stats["switch"] += 1
        kind = rnd.choices(KINDS, KIND_MIX)[0]
        token = sid(s[2], rnd)
        seq = next(next_seq)
        stats[kind] += 1
        if rnd.random() < a.request_share:
            heapq.heappush(pending, (time.monotonic() + rnd.uniform(a.gap_min, a.gap_max), user, seq, kind, token))
        return SendMessage(msg(user=user, sess=s[0], seq=seq, kind=kind, sid=token, rt=s[1],
                               aisle=AISLES[s[2]], ns=now_ns))

    def request(user, needs, kind, token, now_ns):
        stats["requests"] += 1
        return SendMessage(msg(user=user, needs=needs, kind=kind, last=token, surface=SURFACE[kind], ns=now_ns))

    async def send(producer, items, parts):
        # One batch per partition, all sent at once: per-user order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for user, m in items:
            groups[part(user, parts)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def run_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        async with act_p, car_p:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.surge if a.surge_at <= t < a.surge_at + a.surge_for else 1
                owed += a.actions_per_s * x / 100
                now = time.time_ns()
                acts = []
                for _ in range(int(owed)):
                    u = user_id(rnd.randrange(a.users))
                    acts.append((u, action(u, now)))
                owed -= int(owed)
                reqs = []
                while pending and pending[0][0] <= now_s:
                    _, u, needs, kind, token = heapq.heappop(pending)
                    reqs.append((u, request(u, needs, kind, token, time.time_ns())))
                await asyncio.gather(send(act_p, acts, PARTITIONS[ACTIONS]), send(car_p, reqs, PARTITIONS[CAROUSELS]))
                stats["surge"] = x > 1
                due += 0.01                 # from the due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            # Move what survived the last second (sequences, shoppers) out of the collector's reach, so a full
            # collection never walks hundreds of thousands of long-lived objects and pauses the readers.
            gc.freeze()
            phase = "surge" if stats["surge"] else "steady"
            n = sum(stats[k] for k in KINDS)
            print(f"[app] {n:>7,} shopper actions/s ({phase})  views {stats['product_view']:,}  "
                  f"item pages {stats['item_page_visit']:,}  cart adds {stats['add_to_cart']:,}  "
                  f"aisle changes {stats['switch']:,}  carousel requests {stats['requests']:,}  "
                  f"shoppers in session {len(shoppers):,}", flush=True)
            for k in KINDS + ["switch", "requests"]:
                stats[k] = 0

    await asyncio.gather(run_loop(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--users", type=int, default=400_000, help="shoppers in session (illustrative)")
    p.add_argument("--actions-per-s", type=float, default=10_000, help="product views, item page visits and cart adds a second")
    p.add_argument("--switch-share", type=float, default=0.2, help="share of actions where the shopper moves to another aisle")
    p.add_argument("--request-share", type=float, default=0.5, help="share of actions followed by an ads carousel request")
    p.add_argument("--gap-min", type=float, default=0.2, help="seconds from an action to the next carousel request, at least")
    p.add_argument("--gap-max", type=float, default=1.5, help="seconds from an action to the next carousel request, at most")
    p.add_argument("--surge", type=float, default=2.0, help="rate multiplier on the busiest evening")
    p.add_argument("--surge-at", type=float, default=0, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
