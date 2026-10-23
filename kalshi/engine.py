"""Kalshi's matching engine shards: every fill in an order group onto the lane, and every trigger applied before the next fill.

Plays --shards matching engine shards in one process (0 Default, 1 Combos, 2 Crypto, 3 Sports, as Kalshi
assigns them). Market makers' order groups rest orders on two or three shards each: one game's single-game
markets on its home shard and the combos that include it on shard 1. Every fill of an order in a group is
published to the order-group-fills topic, keyed by order group, from the shard that matched it:
  * shard, group, market, the contracts in this fill, a per-(shard, group) sequence and the shard's running
    total of contracts filled for that group (the resulting value, so a repeat or replay changes nothing).
Each shard also reads order-group-states directly on its own connection and applies it to its own view: a
TRIGGERED group has its resting orders on that shard cancelled and takes no new fills there until a RESET.
A shard never sees another shard's view, so a trigger reaches every shard through the log.

Prints fills and contracts per second by shard, fills refused because their group was triggered, and
state-to-shard p50/p99: the second hop of the loop.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict
from contextlib import AsyncExitStack
from datetime import timedelta
from itertools import accumulate

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, FILLS, MARKETS, PARTITIONS, SHARDS, STATES, STREAM, Window, ensure_topics, group,
                    load, msg, part, shards_of, weight)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), FILLS, STATES)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    shards = list(range(a.shards))
    producers = {s: await (await own_client()).producer(STREAM, FILLS, topic_partitions_count=PARTITIONS[FILLS],
                                                         mode=cfg) for s in shards}
    start_ns = time.time_ns()
    groups = [group(i) for i in range(a.groups)]
    on = {g: [s for s in shards_of(g) if s in shards] or [0] for g in groups}
    cumw = list(accumulate(weight(g) for g in groups))
    pid = {g: part(g, PARTITIONS[FILLS]) for g in groups}
    view = {s: {} for s in shards}          # shard -> {group: (state seq, state)}
    seq, cum = defaultdict(int), defaultdict(int)
    lat, stats = Window(), defaultdict(float)
    by_shard = defaultdict(int)

    async def read_states(s):
        # One partition, read directly from the start of the log (each shard rebuilds its view from it), then
        # from its own position. Newest state per group wins, so replays and repeats change nothing.
        c = await own_client()
        who = Consumer.Single(f"shard-{s}-{os.getpid()}")
        nxt = None
        while True:
            ms = await c.poll_messages(STREAM, STATES, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            now = time.time_ns()
            for m in ms:
                d = load(m.payload())
                v = view[s].get(d["g"])
                if v and d["seq"] <= v[0]:
                    continue
                view[s][d["g"]] = (d["seq"], d["state"])
                if d["ns"] > start_ns:      # live states only, not ones replayed at start-up
                    lat.add((now - d["ns"]) / 1e6)
                    if d["state"] == "triggered" and s in on[d["g"]]:
                        stats["cancels"] += 1   # this shard cancels the group's resting orders
            if ms:
                nxt = ms[-1].offset() + 1
            if len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)

    def triggered(s, g):
        v = view[s].get(g)
        return bool(v) and v[1] == "triggered"

    async def fill_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        async with AsyncExitStack() as stack:
            for p in producers.values():
                await stack.enter_async_context(p)
            while True:
                t = time.monotonic() - t0
                x = a.burst if a.burst_at <= t < a.burst_at + a.burst_for else 1
                owed += a.fills_per_s * x / 100
                batches = defaultdict(list)
                for g in random.choices(groups, cum_weights=cumw, k=int(owed)):
                    s = random.choice(on[g])
                    if triggered(s, g):
                        stats["refused"] += 1   # the group's orders on this shard were cancelled
                        continue
                    c = random.randint(1, 49)
                    seq[(s, g)] += 1
                    cum[(s, g)] += c
                    batches[(s, pid[g])].append(SendMessage(msg(sh=s, g=g, mkt=random.choice(MARKETS[s]), c=c,
                                                                seq=seq[(s, g)], cum=cum[(s, g)])))
                    stats["fills"] += 1
                    stats["contracts"] += c
                    by_shard[s] += 1
                owed -= int(owed)
                # One batch per (shard, partition), all sent at once: a group's fills from one shard stay in order.
                if batches:
                    await asyncio.gather(*(producers[s].send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for (s, q), ms in batches.items()))
                stats["burst"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "burst" if stats["burst"] else "steady"
            per = "  ".join(f"{SHARDS[s].lower()} {by_shard[s]:,}" for s in shards)
            n_trig = sum(1 for g in groups if triggered(on[g][0], g))
            print(f"[engine] {int(stats['fills']):>6,} fills/s ({phase})  {int(stats['contracts']):,} contracts/s  "
                  f"by shard: {per}  refused, group triggered {int(stats['refused']):,}  "
                  f"groups triggered now {n_trig:,}  state-to-shards n {r['n']:,} p50 {r['p50']:.1f}ms "
                  f"p99 {r['p99']:.1f}ms", flush=True)
            for k in ("fills", "contracts", "refused", "cancels"):
                stats[k] = 0
            by_shard.clear()

    gc.freeze()                             # keep start-up objects out of the collector's way
    await asyncio.gather(fill_loop(), report(), *(read_states(s) for s in shards))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--shards", type=int, default=4, help="matching engine shards (0 Default, 1 Combos, 2 Crypto, 3 Sports)")
    p.add_argument("--groups", type=int, default=20_000, help="order groups with resting orders; must match tracker.py")
    p.add_argument("--fills-per-s", type=float, default=8_000, help="fills in order groups per second, all shards; must match tracker.py")
    p.add_argument("--burst", type=float, default=1, help="fill-rate multiplier during the burst (a game's final minutes)")
    p.add_argument("--burst-at", type=float, default=0, help="seconds after start")
    p.add_argument("--burst-for", type=float, default=60, help="seconds")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between polls of the states partition")
    asyncio.run(main(p.parse_args()))
