"""Users of a brand's app on CleverTap: decisions delivered to them, then what they do next.

Plays the apps and Experience Builder's delivery log. Each record is sent, keyed by user, to:
  * decision-outcomes: the lane. Either a delivered decision (the action: a campaign with its channel and
    offer, or no action) or a goal event the marketer named (add_to_cart and checkout as positive signals,
    uninstall and subscription_cancel as negative ones), with a sequence number that only grows per user,
    for the online learner;
  * decision-requests: a short time after a record, the user's next decision request (an app open asking
    for in-app or native display content, or the next journey step). It carries the sequence of the
    user's last record, which the learner must already hold, and the action the engine should choose after it.

--gap-min/--gap-max set the time from a record to the next decision request; --surge multiplies the rate
for --surge-for seconds (as when an IPL over ends and a sports app's users all come back at once).
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (KINDS, OUTCOMES, PARTITIONS, REQUESTS, STREAM, DEFAULT_URL, decide, ensure_topics, fold,
                    msg, part, user_id)

KIND_MIX = [0.40, 0.30, 0.18, 0.04, 0.08]   # delivered, add_to_cart, checkout, uninstall, cancel (illustrative)


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), OUTCOMES, REQUESTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    out_p = await (await own_client()).producer(STREAM, OUTCOMES, topic_partitions_count=PARTITIONS[OUTCOMES], mode=cfg)
    req_p = await (await own_client()).producer(STREAM, REQUESTS, topic_partitions_count=PARTITIONS[REQUESTS], mode=cfg)
    # Sequence: one counter for all users, started from the clock, so each user's sequence only grows,
    # even across restarts of this script, without keeping a counter per user.
    next_seq = iter(range(time.time_ns(), 1 << 63))
    users = {}                              # user -> state, as the learner should hold it
    pending = []                            # (when, user, needs, next action, moved): the next decision request
    stats = defaultdict(int)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def record(user, now_ns):
        st = users.get(user)
        before = decide(user, st)           # what the engine would choose before this record
        kind = random.choices(KINDS, KIND_MIX)[0]
        extra = {}
        if kind == "delivered":
            extra["action"] = before        # the engine delivered its decision; it counts toward the cap
        users[user] = st = fold(st, kind, extra.get("action", ""), now_ns)
        after = decide(user, st)
        seq = next(next_seq)
        stats[kind] += 1
        if random.random() < a.request_share:
            heapq.heappush(pending, (time.monotonic() + random.uniform(a.gap_min, a.gap_max), user, seq, after,
                                     before != after))
        return SendMessage(msg(user=user, seq=seq, kind=kind, ns=now_ns, **extra))

    def request(user, needs, nxt, moved, now_ns):
        stats["requests"] += 1
        return SendMessage(msg(user=user, needs=needs, next=nxt, moved=moved, ns=now_ns))

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
        async with out_p, req_p:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.surge if a.surge_at <= t < a.surge_at + a.surge_for else 1
                owed += a.outcomes_per_s * x / 100
                now = time.time_ns()
                recs = []
                for _ in range(int(owed)):
                    u = user_id(random.randrange(a.users))
                    recs.append((u, record(u, now)))
                owed -= int(owed)
                reqs = []
                while pending and pending[0][0] <= now_s:
                    _, u, needs, nxt, moved = heapq.heappop(pending)
                    reqs.append((u, request(u, needs, nxt, moved, time.time_ns())))
                await asyncio.gather(send(out_p, recs, PARTITIONS[OUTCOMES]), send(req_p, reqs, PARTITIONS[REQUESTS]))
                stats["surge"] = x > 1
                due += 0.01                 # from the due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            # Move what survived the last second (users and their state) out of the collector's reach, so a
            # full collection never walks hundreds of thousands of long-lived objects and pauses the producers.
            gc.freeze()
            phase = "surge" if stats["surge"] else "steady"
            n = sum(stats[k] for k in KINDS)
            print(f"[apps] {n:>7,} outcomes/s ({phase})  delivered {stats['delivered']:,}  add to cart "
                  f"{stats['add_to_cart']:,}  checkout {stats['checkout']:,}  uninstall {stats['uninstall']:,}  "
                  f"subscription cancel {stats['subscription_cancel']:,}  decision requests {stats['requests']:,}  "
                  f"users {len(users):,}", flush=True)
            for k in KINDS + ["requests"]:
                stats[k] = 0

    await asyncio.gather(run_loop(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--users", type=int, default=2_000_000, help="users of one brand's app (illustrative)")
    p.add_argument("--outcomes-per-s", type=float, default=20_000, help="delivered decisions and goal events a second")
    p.add_argument("--request-share", type=float, default=0.5, help="share of records followed by a decision request")
    p.add_argument("--gap-min", type=float, default=0.3, help="seconds from a record to the next decision request, at least")
    p.add_argument("--gap-max", type=float, default=2.0, help="seconds from a record to the next decision request, at most")
    p.add_argument("--surge", type=float, default=3.0, help="rate multiplier when an IPL over ends")
    p.add_argument("--surge-at", type=float, default=0, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
