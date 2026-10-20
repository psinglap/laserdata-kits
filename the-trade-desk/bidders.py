"""A fleet of bid servers: every win's spend onto the lane, and every pacing decision applied before the next bid.

Plays --servers bid servers in one process. Each one wins impressions for ad groups it is still allowed to
bid on and, for every win, publishes to the spend topic, keyed by ad group:
  * bid server, ad group, flight, a per-(server, ad group) sequence and the server's running spend for that
    ad group in this flight (the resulting total in USD, never only the price of the last win).
Each bid server also reads pacing-decisions on its own connection and consumer group, and applies them to
its own view: PAUSE stops bidding on the ad group, THROTTLE bids on about half the opportunities, OPEN
resumes. A bid server never sees another's view, so a pause reaches every server through the log.

Prints wins and spend per second, opportunities withheld by pacing, and decision-to-bid-server p50/p99:
the second hop of the loop.
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

from common import (CPM_HIGH, CPM_LOW, DEFAULT_URL, PACING, PARTITIONS, SPEND, STREAM, Window, ad_group,
                    ensure_topics, flight, load, msg, part)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SPEND, PACING)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, SPEND, topic_partitions_count=PARTITIONS[SPEND], mode=cfg)
    start_ns = time.time_ns()
    servers = [f"bid-{i:02d}" for i in range(a.servers)]
    groups = [ad_group(i) for i in range(a.ad_groups)]
    pid = {g: part(g, PARTITIONS[SPEND]) for g in groups}
    view = {b: {} for b in servers}         # bid server -> {ad group: (flight, action, ns)}
    seq, cum = defaultdict(int), defaultdict(float)
    lat, stats = Window(), defaultdict(float)

    def reader(b):
        async def on_decision(m):
            d = load(m.payload())
            v = view[b].get(d["g"])
            if v and d["ns"] <= v[2]:
                return                      # older or repeated decision: the newest wins
            view[b][d["g"]] = (d["flight"], d["action"], d["ns"])
            if d["ns"] > start_ns and d["flight"] == flight(a.flight_s):
                lat.add((time.time_ns() - d["ns"]) / 1e6)   # live decisions only, not ones replayed at start-up
        return on_decision

    readers = []
    for b in servers:
        readers.append((await (await own_client()).consumer_group(
            f"{b}-{os.getpid()}", STREAM, PACING, polling_strategy=PollingStrategy.First(), batch_length=1000,
            auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500)), reader(b)))
    gc.freeze()                             # keep start-up objects out of the collector's way

    def allowed(b, g, f):
        v = view[b].get(g)
        if not v or v[0] != f or v[1] == "open":
            return True
        return v[1] == "throttle" and random.random() < 0.5

    async def win_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        async with producer:
            while True:
                t = time.monotonic() - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.wins_per_s * x / 100
                f = flight(a.flight_s)
                batches = defaultdict(list)
                for _ in range(int(owed)):
                    b = random.choice(servers)
                    for _ in range(4):      # an opportunity for an ad group this server may still bid on
                        g = random.choice(groups)
                        if allowed(b, g, f):
                            break
                        stats["withheld"] += 1
                    else:
                        continue
                    price = random.uniform(CPM_LOW, CPM_HIGH) / 1000
                    seq[(b, g)] += 1
                    cum[(f, b, g)] += price
                    batches[pid[g]].append(SendMessage(msg(b=b, g=g, flight=f, seq=seq[(b, g)],
                                                           spend=round(cum[(f, b, g)], 6))))
                    stats["wins"] += 1
                    stats["usd"] += price
                owed -= int(owed)
                # One batch per partition, all sent at once: per-ad-group order is kept, about one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            f = flight(a.flight_s)
            paused = sum(1 for g, v in view[servers[0]].items() if v[0] == f and v[1] == "pause")
            phase = "peak" if stats["peak"] else "steady"
            print(f"[bidders] {int(stats['wins']):>7,} wins/s ({phase})  spend ${stats['usd']:,.2f}/s  "
                  f"withheld by pacing {int(stats['withheld']):,}  paused in {servers[0]}'s view {paused:,}  "
                  f"decision-to-bid-servers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("wins", "usd", "withheld"):
                stats[k] = 0
            old = f - 2
            for k in [k for k in cum if k[0] <= old]:
                del cum[k]

    await asyncio.gather(win_loop(), report(), *(c.consume_messages(cb, asyncio.Event()) for c, cb in readers))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--servers", type=int, default=8, help="bid servers, each with its own pacing reader")
    p.add_argument("--ad-groups", type=int, default=5_000, help="must match pacing.py")
    p.add_argument("--wins-per-s", type=float, default=10_000, help="won impressions per second, all servers")
    p.add_argument("--flight-s", type=float, default=60, help="seconds per compressed flight; must match pacing.py")
    p.add_argument("--peak", type=float, default=1, help="win-rate multiplier during the peak (a live-sports moment)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
