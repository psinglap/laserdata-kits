"""A fleet of API servers: every billable request's usage onto the lane, and every stop applied before the next request.

Plays --servers API servers in one process. Each one serves billable requests (agent model calls charged as
paid additional usage) for users it is still allowed to serve and, for every request, publishes to the
usage topic, keyed by user:
  * API server, user, billing period, a per-(server, user) sequence and the server's running paid usage for
    that user in this period (the resulting total in USD, never only the cost of the last request).
Each API server also reads limit-decisions on its own connection and consumer group, and applies them to its
own view: STOP refuses that user's paid additional usage for the rest of the period; ALERT is informational.
An API server never sees another's view, so a stop reaches every server through the log.

Prints requests and paid usage per second, requests refused at the limit, and decision-to-API-server
p50/p99: the second hop of the loop.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (COST_HIGH, COST_LOW, DECISIONS, DEFAULT_URL, PARTITIONS, STREAM, USAGE, Window,
                    ensure_topics, load, msg, part, period, user)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), USAGE, DECISIONS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, USAGE, topic_partitions_count=PARTITIONS[USAGE], mode=cfg)
    start_ns = time.time_ns()
    servers = [f"api-{i:02d}" for i in range(a.servers)]
    users = [user(i) for i in range(a.users)]
    pid = {u: part(u, PARTITIONS[USAGE]) for u in users}
    view = {s: {} for s in servers}         # API server -> {user: (period, action, ns)}
    seq, cum = defaultdict(int), defaultdict(float)
    lat, stats = Window(), defaultdict(float)

    def reader(s):
        async def on_decision(m):
            d = load(m.payload())
            v = view[s].get(d["u"])
            if v and d["ns"] <= v[2]:
                return                      # older or repeated decision: the newest wins
            view[s][d["u"]] = (d["p"], d["action"], d["ns"])
            if d["ns"] > start_ns and d["p"] == period(a.period_s):
                lat.add((time.time_ns() - d["ns"]) / 1e6)   # live decisions only, not ones replayed at start-up
        return on_decision

    readers = []
    for s in servers:
        readers.append((await (await own_client()).consumer_group(
            f"{s}-{os.getpid()}", STREAM, DECISIONS, polling_strategy=PollingStrategy.First(), batch_length=1000,
            auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500)), reader(s)))
    gc.freeze()                             # keep start-up objects out of the collector's way

    def stopped(s, u, p):
        v = view[s].get(u)
        return bool(v) and v[0] == p and v[1] == "stop"

    async def request_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        async with producer:
            while True:
                t = time.monotonic() - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.requests_per_s * x / 100
                p = period(a.period_s)
                batches = defaultdict(list)
                for _ in range(int(owed)):
                    s = random.choice(servers)
                    u = random.choice(users)
                    if stopped(s, u, p):
                        stats["refused"] += 1   # paid additional usage stopped for this user
                        continue
                    cost = random.uniform(COST_LOW, COST_HIGH)
                    seq[(s, u)] += 1
                    cum[(p, s, u)] += cost
                    batches[pid[u]].append(SendMessage(msg(s=s, u=u, p=p, seq=seq[(s, u)],
                                                           usd=round(cum[(p, s, u)], 6))))
                    stats["requests"] += 1
                    stats["usd"] += cost
                owed -= int(owed)
                # One batch per partition, all sent at once: per-user order is kept, about one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            p = period(a.period_s)
            n_stopped = sum(1 for v in view[servers[0]].values() if v[0] == p and v[1] == "stop")
            phase = "peak" if stats["peak"] else "steady"
            print(f"[api] {int(stats['requests']):>7,} billable requests/s ({phase})  paid usage ${stats['usd']:,.2f}/s  "
                  f"refused at the limit {int(stats['refused']):,}  stopped in {servers[0]}'s view {n_stopped:,}  "
                  f"decision-to-api-servers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("requests", "usd", "refused"):
                stats[k] = 0
            old = p - 2
            for k in [k for k in cum if k[0] <= old]:
                del cum[k]

    await asyncio.gather(request_loop(), report(), *(c.consume_messages(cb, asyncio.Event()) for c, cb in readers))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--servers", type=int, default=8, help="API servers, each with its own decisions reader")
    p.add_argument("--users", type=int, default=20_000, help="must match limits.py")
    p.add_argument("--requests-per-s", type=float, default=10_000, help="billable requests per second, all servers")
    p.add_argument("--period-s", type=float, default=60, help="seconds per compressed billing period; must match limits.py")
    p.add_argument("--peak", type=float, default=1, help="request-rate multiplier during the peak (a model launch day)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
