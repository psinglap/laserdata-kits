"""A fleet of exchange nodes: every auction's outcome onto the lane, and every new floor applied before the next auction.

Plays --nodes exchange nodes in one process. Each auction is for one inventory segment (content category x
device type x break slot). The bids that came back in time are cleared second-price against the node's
current floor for that segment, and the outcome is published to auction-outcomes, keyed by segment:
  * node, segment, a per-(node, segment) sequence, how many bids came back, the top and the second bid.
Each node also reads floor-updates on its own connection, straight from the partition (no consumer group),
into its own floor table; a node never sees another's table, so a floor reaches every node through the log.

The business number: every auction is also cleared, from the same bids, against the floor the node held at
the last periodic refresh (every --refresh-s seconds), the floors a slower refresh path would have used. Both
are judged against the same bids and the same pre-auction table, and the difference is printed each second as
revenue per thousand auctions (RPM) and fill rate.

With --join-at, one more node starts later and rebuilds its whole floor table from the log before it takes
auctions, the way a node added during a live game would.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, FLOORS, LIVE, OUTCOMES, PARTITIONS, STREAM, Window, bids, clear, ensure_topics,
                    load, msg, part, segments)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), OUTCOMES, FLOORS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, OUTCOMES, topic_partitions_count=PARTITIONS[OUTCOMES],
                                                   mode=cfg)
    start_ns = time.time_ns()
    nodes = [f"x-{i:02d}" for i in range(a.nodes)]
    segs = segments(a.segments)
    rng = random.Random(7)
    weights = [1 / (i + 1) ** 0.8 for i in range(len(segs))]     # a few segments carry most of the traffic
    rng.shuffle(weights)
    pid = {s: part(s, PARTITIONS[OUTCOMES]) for s in segs}
    view = {x: {} for x in nodes}           # node -> {segment: (floor, seq)}
    snap = {x: {} for x in nodes}           # node -> floors as of the last periodic refresh
    refreshed = [-1]
    seq = defaultdict(int)
    lat, stats, total = Window(), defaultdict(float), defaultdict(float)
    taking = list(nodes)                    # nodes taking auctions (a late node joins once rebuilt)

    def reader(x):
        def on_floor(m):
            f = load(m.payload())
            v = view[x].get(f["s"])
            if v and f["seq"] <= v[1]:
                return                      # older or repeated floor: the newest wins
            view[x][f["s"]] = (f["floor"], f["seq"])
            if f["ns"] > start_ns and x != "late":
                lat.add((time.time_ns() - f["ns"]) / 1e6)   # live floors only, not ones replayed at start-up
        return on_floor

    async def read(c, x, on_floor):
        # The partition from the start of the log (the floor table is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        pids = range(PARTITIONS[FLOORS])
        who = {p: Consumer.Single(f"{x}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, FLOORS, consumer=who[p], partition_id=p, count=1000,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_floor(m)
                    more = more or len(ms) == 1000
            if not more:
                await asyncio.sleep(0.0005)

    readers = [(await own_client(), x, reader(x)) for x in nodes]   # one connection per node's floor reader
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def late_node():
        # A node added mid-game: it reads the floor log from the start, and takes auctions once its table
        # holds a floor for every segment x-00 has priced.
        await asyncio.sleep(a.join_at)
        x = "late"
        view[x], snap[x] = {}, {}
        t0 = time.monotonic()
        task = asyncio.ensure_future(read(await own_client(), x, reader(x)))
        while len(view[x]) < len(view[nodes[0]]):
            await asyncio.sleep(0.005)
        ms = (time.monotonic() - t0) * 1000
        n = sum(v[1] for v in view[x].values())
        print(f"[join] late node rebuilt {len(view[x]):,} segment floors from {n:,} floor updates in the log "
              f"in {ms:.0f} ms, now taking auctions", flush=True)
        taking.append(x)
        await task

    async def auction_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        async with producer:
            while True:
                t = time.monotonic() - t0
                owed += a.auctions_per_s / 100
                r = int(time.time() // a.refresh_s)
                if r != refreshed[0]:       # the periodic refresh: what a slower floor path would hand the node
                    refreshed[0] = r
                    for x in view:
                        snap[x] = {s: v[0] for s, v in view[x].items()}
                batches = defaultdict(list)
                for s in rng.choices(segs, weights, k=int(owed)):
                    x = rng.choice(taking)
                    b = bids(s, t, a, rng)
                    top = b[0] if b else 0.0
                    second = b[1] if len(b) > 1 else 0.0
                    live = view[x].get(s, (0.0, 0))[0]      # both judged on the same table, before any update
                    paid = clear(top, second, live)
                    stats["auctions"] += 1
                    if snap[x]:                 # compared once the refreshed table holds floors at all
                        old = clear(top, second, snap[x].get(s, 0.0))
                        for k, v in (("n", 1), ("rev", paid), ("rev_old", old), ("fill", paid > 0), ("fill_old", old > 0)):
                            stats[k] += v
                            total[k] += v
                        if s.startswith(LIVE + "/"):
                            for k, v in (("live_rev", paid), ("live_rev_old", old)):
                                stats[k] += v
                    seq[(x, s)] += 1
                    batches[pid[s]].append(SendMessage(msg(x=x, s=s, seq=seq[(x, s)], n=len(b), top=round(top, 4),
                                                           sec=round(second, 4))))
                owed -= int(owed)
                # One batch per partition, all sent at once: per-segment order is kept, about one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["surge"] = t % a.surge_every < a.surge_for
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    def rpm(rev, n):
        # Prices are CPM, so one impression earns price/1000 USD; per thousand auctions that is sum(price)/n.
        return rev / n if n else 0.0

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            n, tn = stats["n"], total["n"]
            phase = f"{LIVE} break" if stats["surge"] else "steady"
            gap = 100 * (stats["rev"] / stats["rev_old"] - 1) if stats["rev_old"] else 0.0
            tgap = 100 * (total["rev"] / total["rev_old"] - 1) if total["rev_old"] else 0.0
            lgap = 100 * (stats["live_rev"] / stats["live_rev_old"] - 1) if stats["live_rev_old"] else 0.0
            print(f"[exchange] {int(stats['auctions']):>7,} auctions/s ({phase})  nodes {len(taking)}  floors in {nodes[0]}'s table "
                  f"{len(view[nodes[0]]):,}  floor-to-exchange-nodes n {r['n']:,} p50 {r['p50']:.1f}ms "
                  f"p99 {r['p99']:.1f}ms", flush=True)
            print(f"[yield] live floors: RPM ${rpm(stats['rev'], n):.2f} fill {100 * stats['fill'] / n if n else 0:.1f}%  "
                  f"| floors refreshed every {a.refresh_s:g} s: RPM ${rpm(stats['rev_old'], n):.2f} "
                  f"fill {100 * stats['fill_old'] / n if n else 0:.1f}%  | live vs refreshed {gap:+.1f}% "
                  f"({LIVE} {lgap:+.1f}%)  run total {tgap:+.2f}% on ${total['rev'] / 1000:,.2f}", flush=True)
            stats.clear()

    tasks = [auction_loop(), report(), *(read(c, x, cb) for c, x, cb in readers)]
    if a.join_at:
        tasks.append(late_node())
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--nodes", type=int, default=8, help="exchange nodes, each with its own floor reader")
    p.add_argument("--segments", type=int, default=400, help="inventory segments (up to 400 in common.py)")
    p.add_argument("--auctions-per-s", type=float, default=10_000, help="auctions per second, all nodes")
    p.add_argument("--density", type=float, default=2.5, help="average bids back in time per auction, before surges")
    p.add_argument("--surge", type=float, default=2.0, help="demand multiplier in the live category's ad breaks")
    p.add_argument("--surge-every", type=float, default=90, help="seconds between live ad breaks")
    p.add_argument("--surge-for", type=float, default=30, help="seconds each live ad break lasts")
    p.add_argument("--refresh-s", type=float, default=300, help="the slower floor refresh to compare against")
    p.add_argument("--join-at", type=float, default=0, help="add a node this many seconds in (0 = no late node)")
    asyncio.run(main(p.parse_args()))
