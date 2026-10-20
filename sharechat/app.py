"""The apps: users scroll the feed, the app flushes the posts they viewed, then asks for the next page.

Plays the ShareChat and Moj clients. The app tracks which posts the user actually viewed and sends
them to the backend in small batches (a flush). Each flush is published, keyed by user, to:
  * viewed-posts: the lane. The post IDs viewed since the last flush, with a sequence
    number that only grows for each user, for the dedup service in front of ranking;
  * feed-requests: a short time after a flush, the user's next feed request. It carries the
    sequence of the user's last flush, which the dedup service must already hold, and a candidate
    list from retrieval that includes some of the posts the user has just viewed (popular posts
    come back from retrieval again).

--gap-min/--gap-max set the time from a flush to the next feed request; --peak multiplies the
rate for --peak-for seconds (festival evenings).
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

from common import DEFAULT_URL, PARTITIONS, REQUESTS, STREAM, VIEWED, ensure_topics, msg, part, post_id, user_id


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), VIEWED, REQUESTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    view_p = await (await own_client()).producer(STREAM, VIEWED, topic_partitions_count=PARTITIONS[VIEWED], mode=cfg)
    req_p = await (await own_client()).producer(STREAM, REQUESTS, topic_partitions_count=PARTITIONS[REQUESTS], mode=cfg)
    # Flush sequence: one counter for all users, started from the clock, so each user's sequence only
    # grows, even across restarts of this script, without keeping a counter per user.
    next_seq = iter(range(time.time_ns(), 1 << 63))
    pending = []                            # (when, user, needs, just viewed): the next feed request after a flush
    stats = defaultdict(int)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def flush(user, now):
        posts = [post_id(random.randrange(a.posts)) for _ in range(a.posts_per_flush)]
        s = next(next_seq)
        stats["flushes"] += 1
        stats["views"] += len(posts)
        if random.random() < a.request_share:
            heapq.heappush(pending, (time.monotonic() + random.uniform(a.gap_min, a.gap_max), user, s,
                                     random.sample(posts, min(a.repeats, len(posts)))))
        return SendMessage(msg(user=user, seq=s, posts=posts, ns=now))

    def request(user, needs, just, now):
        cands = [post_id(random.randrange(a.posts)) for _ in range(a.candidates - len(just))] + just
        stats["requests"] += 1
        return SendMessage(msg(user=user, needs=needs, cands=cands, just=just, ns=now))

    async def send(producer, items, parts):
        # One batch per partition, all sent at once: per-user order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for user, m in items:
            groups[part(user, parts)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def run():
        t0 = due = time.monotonic()
        owed = 0.0
        async with view_p, req_p:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.views_per_s * x / a.posts_per_flush / 100
                now = time.time_ns()
                views = []
                for _ in range(int(owed)):
                    u = user_id(random.randrange(a.users))
                    views.append((u, flush(u, now)))
                owed -= int(owed)
                reqs = []
                while pending and pending[0][0] <= now_s:
                    _, u, needs, just = heapq.heappop(pending)
                    reqs.append((u, request(u, needs, just, time.time_ns())))
                await asyncio.gather(send(view_p, views, PARTITIONS[VIEWED]), send(req_p, reqs, PARTITIONS[REQUESTS]))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "peak" if stats["peak"] else "steady"
            print(f"[app] {stats['views']:>7,} views/s ({phase})  in {stats['flushes']:,} flushes  "
                  f"next-page feed requests {stats['requests']:,}", flush=True)
            for k in ("views", "flushes", "requests"):
                stats[k] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--users", type=int, default=20_000_000, help="daily active users (Moj, P99 CONF 2023)")
    p.add_argument("--posts", type=int, default=1_000_000, help="posts in circulation")
    p.add_argument("--views-per-s", type=float, default=70_000, help="about 6 billion Moj video plays a day")
    p.add_argument("--posts-per-flush", type=int, default=5, help="viewed posts the app sends per flush")
    p.add_argument("--request-share", type=float, default=0.55, help="share of flushes followed by a feed request")
    p.add_argument("--gap-min", type=float, default=0.2, help="seconds from a flush to the next feed request, at least")
    p.add_argument("--gap-max", type=float, default=2.0, help="seconds from a flush to the next feed request, at most")
    p.add_argument("--candidates", type=int, default=20, help="candidate posts per feed request")
    p.add_argument("--repeats", type=int, default=2, help="of those, posts the user has just viewed")
    p.add_argument("--peak", type=float, default=1.5, help="rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
