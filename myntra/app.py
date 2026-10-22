"""The Myntra app: shoppers search, open products, wishlist, add to bag and filter, then type in the search bar.

Plays the app during a sale window. Each shopper action is sent, keyed by user, to:
  * shopper-actions: the lane. The action's annotations (category, brand, gender, with a weight per
    action type, as Myntra's contextual type-ahead describes them), the shopper's session, and a sequence
    number that only grows for each user, for the session context service;
  * typeahead-requests: a short time after an action, the shopper's next type-ahead request (back to the
    search bar, first letters typed). It carries the sequence of the shopper's last action, which the
    context must already hold, and the category the context should boost first after that action.

Shoppers keep an intent (a category) for a while and sometimes change their mind (--switch-share): a
shopper who was on sneakers opens a kurta, and the next suggestions should lead with kurtas.
--gap-min/--gap-max set the time from an action to the next type-ahead request; --surge multiplies the
rate for --surge-for seconds (the first hour of a sale, when everyone is browsing at once).
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

from common import (ACTION_WEIGHT, ACTIONS, BRANDS, CATEGORIES, DEFAULT_URL, GENDERS, PARTITIONS, STREAM,
                    TYPEAHEAD, ensure_topics, fold, msg, part, top, user_id)

KINDS = list(ACTION_WEIGHT)
KIND_MIX = [0.25, 0.45, 0.1, 0.08, 0.12]        # search, view, wishlist, add_to_bag, filter (illustrative)


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ACTIONS, TYPEAHEAD)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    act_p = await (await own_client()).producer(STREAM, ACTIONS, topic_partitions_count=PARTITIONS[ACTIONS], mode=cfg)
    ta_p = await (await own_client()).producer(STREAM, TYPEAHEAD, topic_partitions_count=PARTITIONS[TYPEAHEAD], mode=cfg)
    # Action sequence: one counter for all users, started from the clock, so each user's sequence only grows,
    # even across restarts of this script, without keeping a counter per user.
    next_seq = iter(range(time.time_ns(), 1 << 63))
    run = os.urandom(3).hex()               # sessions from this run of the app
    shoppers = {}                           # user -> [session, gender, intent category, context]
    pending = []                            # (when, user, needs, top category): the next type-ahead request
    stats = defaultdict(int)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def action(user, now_ns):
        s = shoppers.get(user)
        if s is None:
            s = shoppers[user] = [f"{run}-{user}", random.choice(GENDERS), random.choice(CATEGORIES), None]
        if random.random() < a.switch_share:
            s[2] = random.choice(CATEGORIES)    # changed their mind: a new intent
            stats["switch"] += 1
        kind = random.choices(KINDS, KIND_MIX)[0]
        w = ACTION_WEIGHT[kind]
        ann = [("category", s[2], w), ("brand", random.choice(BRANDS), round(w * 0.7, 2)), ("gender", s[1], 0.5)]
        before = top(s[3])
        s[3] = fold(s[3], s[0], ann, now_ns)     # what the context should hold after this action
        after = top(s[3])
        seq = next(next_seq)
        stats[kind] += 1
        if random.random() < a.request_share:
            heapq.heappush(pending, (time.monotonic() + random.uniform(a.gap_min, a.gap_max), user, seq, after,
                                     before != after))
        return SendMessage(msg(user=user, sess=s[0], seq=seq, kind=kind, ann=ann, ns=now_ns))

    def request(user, needs, boost, moved, now_ns):
        stats["requests"] += 1
        prefix = boost[:2]                  # the first letters typed (illustrative)
        return SendMessage(msg(user=user, needs=needs, top=boost, moved=moved, q=prefix, ns=now_ns))

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
        async with act_p, ta_p:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.surge if a.surge_at <= t < a.surge_at + a.surge_for else 1
                owed += a.actions_per_s * x / 100
                now = time.time_ns()
                acts = []
                for _ in range(int(owed)):
                    u = user_id(random.randrange(a.users))
                    acts.append((u, action(u, now)))
                owed -= int(owed)
                reqs = []
                while pending and pending[0][0] <= now_s:
                    _, u, needs, boost, moved = heapq.heappop(pending)
                    reqs.append((u, request(u, needs, boost, moved, time.time_ns())))
                await asyncio.gather(send(act_p, acts, PARTITIONS[ACTIONS]), send(ta_p, reqs, PARTITIONS[TYPEAHEAD]))
                stats["surge"] = x > 1
                due += 0.01                 # from the due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            # Move what survived the last second (contexts, shoppers) out of the collector's reach, so a full
            # collection never walks hundreds of thousands of long-lived objects and pauses the readers.
            gc.freeze()
            phase = "surge" if stats["surge"] else "steady"
            n = sum(stats[k] for k in KINDS)
            print(f"[app] {n:>7,} shopper actions/s ({phase})  search {stats['search']:,}  view {stats['view']:,}  "
                  f"wishlist {stats['wishlist']:,}  bag {stats['add_to_bag']:,}  filter {stats['filter']:,}  "
                  f"intent changes {stats['switch']:,}  type-ahead requests {stats['requests']:,}  "
                  f"shoppers in session {len(shoppers):,}", flush=True)
            for k in KINDS + ["switch", "requests"]:
                stats[k] = 0

    await asyncio.gather(run_loop(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--users", type=int, default=500_000, help="shoppers in session (illustrative)")
    p.add_argument("--actions-per-s", type=float, default=20_000, help="searches, views, wishlist, bag adds, filters a second")
    p.add_argument("--switch-share", type=float, default=0.3, help="share of actions where the shopper changes intent")
    p.add_argument("--request-share", type=float, default=0.5, help="share of actions followed by a type-ahead request")
    p.add_argument("--gap-min", type=float, default=0.3, help="seconds from an action to the next type-ahead request, at least")
    p.add_argument("--gap-max", type=float, default=2.0, help="seconds from an action to the next type-ahead request, at most")
    p.add_argument("--surge", type=float, default=2.0, help="rate multiplier in the first hour of a sale")
    p.add_argument("--surge-at", type=float, default=0, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
