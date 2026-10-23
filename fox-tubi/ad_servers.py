"""A fleet of ad server nodes: every creative a node has no verdict for goes onto the lane once, and every
verdict the registry publishes is applied on every node before its next ad decision.

Plays --nodes ad server nodes in one process. Each ad request asks for a pod of 1 to --pod slots during an ad
break; programmatic bids come back, each carrying a creative from one of the DSPs. A node can place a bid only
when it holds a "ready" verdict for that bid's creative: transcoded, past the standards check, and with a
known brand and ad category, so competitive separation (no two ads of one category in a pod) can be enforced.

A bid whose creative the node has no verdict for is held, and the first time the node meets that creative it
publishes one record to creative-seen, keyed by creative:
  * node, creative, DSP, and a per-(node, partition) sequence.
Each node also reads creative-ready on its own connection, straight from the partition (no consumer group),
into its own verdict table; a node never sees another's table, so a verdict reaches every node through the log.

New creatives keep arriving (--new-per-s), and every --flight-every seconds a wave of --flight-n new creatives
starts at once, the way new flights start at the top of a live event. The business number: every request is
also decided, from the same bids, with the verdict table each node held at the last periodic refresh (every
--refresh-s seconds), the table a slower cache refresh would hand it. Both are judged on the same bids and the
same pre-request tables, and printed each second as bids held, pod fill and revenue per thousand requests.

With --join-at, one more node starts later and rebuilds its whole verdict table from the log before it takes
requests, the way a node added before a live event would.
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

from common import (DEFAULT_URL, DSPS, PARTITIONS, READY, SEEN, STREAM, Window, base_cpm, creative_id,
                    ensure_topics, h, load, msg, part)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SEEN, READY)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, SEEN, topic_partitions_count=PARTITIONS[SEEN], mode=cfg)
    start_ns = time.time_ns()
    nodes = [f"as-{i:02d}" for i in range(a.nodes)]
    rng = random.Random(7)
    ring = [creative_id(n) for n in range(a.catalog)]   # the creatives DSPs are bidding with right now
    head, born = [0], [a.catalog]
    cpm = {}                                # creative -> typical bid (cached)
    view = {x: {} for x in nodes}           # node -> {creative: (status, category)}
    snap = {x: {} for x in nodes}           # node -> verdicts as of the last periodic refresh
    reported = {x: set() for x in nodes}    # creatives this node has already put on the lane
    seq = defaultdict(int)
    first_bid = {}                          # creative -> wall time of its first bid on any node
    applied = defaultdict(int)              # creative -> nodes that hold its verdict
    lat, stats, total = Window(), defaultdict(float), defaultdict(float)
    lag_live, lag_refresh = Window(), Window()
    taking = list(nodes)                    # nodes taking requests (a late node joins once rebuilt)

    def reader(x):
        def on_verdict(m):
            v = load(m.payload())
            c = v["c"]
            if c in view[x]:
                return                      # repeated verdict: the first one stands
            view[x][c] = (v["status"], v["cat"])
            if v.get("snap") or x == "late":
                return                      # start-up snapshot and rebuilt tables are not timed
            now = time.time_ns()
            if v["ns"] > start_ns:
                lat.add((now - v["ns"]) / 1e6)
            applied[c] += 1
            if applied[c] == len(nodes) and c in first_bid:
                # servable everywhere: since the creative's first bid, live and with a periodic refresh
                t = now / 1e9
                lag_live.add(t - first_bid[c])
                lag_refresh.add((int(t // a.refresh_s) + 1) * a.refresh_s - first_bid[c])
        return on_verdict

    async def read(c, x, on_verdict):
        # The partition from the start of the log (the verdict table is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        pids = range(PARTITIONS[READY])
        who = {p: Consumer.Single(f"{x}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, READY, consumer=who[p], partition_id=p, count=1000,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_verdict(m)
                    more = more or len(ms) == 1000
            if not more:
                await asyncio.sleep(0.0005)

    readers = [(await own_client(), x, reader(x)) for x in nodes]   # one connection per node's verdict reader
    tasks = [asyncio.ensure_future(read(c, x, cb)) for c, x, cb in readers]
    t0 = time.monotonic()
    while min(len(view[x]) for x in nodes) < a.catalog:     # wait for the registry's start-up snapshot
        await asyncio.sleep(0.01)
    print(f"[start] {len(nodes)} nodes hold the {a.catalog:,} approved creatives from the registry's snapshot "
          f"after {(time.monotonic() - t0) * 1000:.0f} ms; taking requests", flush=True)
    for x in nodes:
        snap[x] = dict(view[x])             # the slower path starts from the same table
    refreshed = [int(time.time() // a.refresh_s)]
    gc.freeze()                             # keep start-up objects out of the collector's way

    def new_creatives(n):
        for _ in range(n):
            ring[head[0]] = creative_id(born[0])            # replaces the oldest creative in rotation
            head[0] = (head[0] + 1) % len(ring)
            born[0] += 1

    async def late_node():
        # A node added before a live event: it reads the verdict log from the start, and takes requests once
        # its table holds as many verdicts as as-00's.
        await asyncio.sleep(a.join_at)
        x = "late"
        view[x], snap[x], reported[x] = {}, {}, set()
        t1 = time.monotonic()
        task = asyncio.ensure_future(read(await own_client(), x, reader(x)))
        while len(view[x]) < len(view[nodes[0]]):
            await asyncio.sleep(0.005)
        print(f"[join] late node rebuilt {len(view[x]):,} creative verdicts from the log in "
              f"{(time.monotonic() - t1) * 1000:.0f} ms, now taking requests", flush=True)
        snap[x] = dict(view[x])
        taking.append(x)
        await task

    def decide(bids, table, slots):
        # Highest bids first; a bid needs a ready verdict, and one ad per category per pod.
        placed, cats, rev = 0, set(), 0.0
        for price, c in bids:
            v = table.get(c)
            if v is None or v[0] != "ready" or v[1] in cats:
                continue
            cats.add(v[1])
            rev += price / 1000
            placed += 1
            if placed == slots:
                break
        return placed, rev

    async def request_loop():
        start = due = time.monotonic()
        owed = 0.0
        async with producer:
            while True:
                t = time.monotonic() - start
                r = int(time.time() // a.refresh_s)
                if r != refreshed[0]:       # the periodic refresh: what a slower cache path hands the node
                    refreshed[0] = r
                    for x in taking:
                        snap[x] = dict(view[x])
                new_creatives(int(a.new_per_s * (t + 0.01)) - int(a.new_per_s * t))
                wave = a.flight_every and int((t + 0.01) / a.flight_every) != int(t / a.flight_every)
                if wave:
                    new_creatives(a.flight_n)
                    stats["wave"] = 1
                owed += a.requests_per_s / 100
                now = time.time()
                batches = defaultdict(list)
                for _ in range(int(owed)):
                    x = rng.choice(taking)
                    bids = []
                    for _ in range(rng.randint(1, 2 * a.bids - 1)):
                        c = rng.choice(ring)
                        p = cpm.get(c) or cpm.setdefault(c, base_cpm(c))
                        bids.append((p * rng.lognormvariate(0, 0.3), c))
                        first_bid.setdefault(c, now)
                        v = view[x].get(c)
                        stats["bids"] += 1
                        if v is None:
                            stats["wait"] += 1
                            if c not in reported[x]:            # first time this node meets it: onto the lane
                                reported[x].add(c)
                                pid = part(c, PARTITIONS[SEEN])
                                seq[(x, pid)] += 1              # ordered within a partition, so per (node, partition)
                                batches[pid].append(
                                    SendMessage(msg(x=x, c=c, d=DSPS[h(c, "dsp") % len(DSPS)], seq=seq[(x, pid)])))
                        if snap[x].get(c) is None:
                            stats["wait_old"] += 1
                    bids.sort(reverse=True)
                    slots = rng.randint(1, a.pod)
                    live, rev = decide(bids, view[x], slots)        # both judged on the same bids and
                    old, rev_old = decide(bids, snap[x], slots)     # the same pre-request tables
                    for k, v in (("req", 1), ("slots", slots), ("fill", live), ("fill_old", old), ("rev", rev),
                                 ("rev_old", rev_old)):
                        stats[k] += v
                        total[k] += v
                owed -= int(owed)
                # One batch per partition, all sent at once: a creative's sightings keep their order, about one
                # round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                    stats["seen"] += sum(len(ms) for ms in batches.values())
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            n, tn = stats["req"] or 1, total["req"] or 1
            phase = "flight wave" if stats["wave"] else "steady"
            print(f"[ad-servers] {int(stats['req']):>6,} requests/s ({phase})  nodes {len(taking)}  verdicts in "
                  f"{nodes[0]}'s table {len(view[nodes[0]]):,}  sightings sent {int(stats['seen']):,}  "
                  f"verdict-to-ad-servers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            b = stats["bids"] or 1
            gap = 100 * (stats["rev"] / stats["rev_old"] - 1) if stats["rev_old"] else 0.0
            tgap = 100 * (total["rev"] / total["rev_old"] - 1) if total["rev_old"] else 0.0
            print(f"[yield] live verdicts: bids held {100 * stats['wait'] / b:.1f}%  pod fill "
                  f"{100 * stats['fill'] / (stats['slots'] or 1):.1f}%  RPM ${1000 * stats['rev'] / n:.2f}  "
                  f"| verdicts refreshed every {a.refresh_s:g} s: bids held {100 * stats['wait_old'] / b:.1f}%  "
                  f"pod fill {100 * stats['fill_old'] / (stats['slots'] or 1):.1f}%  "
                  f"RPM ${1000 * stats['rev_old'] / n:.2f}  | live vs refreshed {gap:+.1f}%  run total {tgap:+.2f}% "
                  f"on ${total['rev']:,.2f}", flush=True)
            ll, lr = lag_live.flush(), lag_refresh.flush()
            if ll["n"]:
                print(f"[new] {ll['n']:,} new creatives servable on all {len(nodes)} nodes this second  first bid "
                      f"to servable everywhere p50 {ll['p50']:.1f}s (job ~{a.process_s:g}s + lane)  "
                      f"| with a {a.refresh_s:g} s refresh p50 {lr['p50']:.1f}s", flush=True)
            stats.clear()

    tasks += [asyncio.ensure_future(request_loop()), asyncio.ensure_future(report())]
    if a.join_at:
        tasks.append(asyncio.ensure_future(late_node()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--nodes", type=int, default=8, help="ad server nodes, each with its own verdict reader")
    p.add_argument("--requests-per-s", type=float, default=5000, help="ad requests (pods) per second, all nodes")
    p.add_argument("--bids", type=int, default=5, help="average programmatic bids back per request")
    p.add_argument("--pod", type=int, default=5, help="most slots in one ad pod (each request asks 1 to this)")
    p.add_argument("--catalog", type=int, default=6000, help="creatives in rotation (same as creative_registry.py)")
    p.add_argument("--new-per-s", type=float, default=30, help="new creatives entering rotation per second")
    p.add_argument("--flight-every", type=float, default=120, help="seconds between waves of new flights (0 = none)")
    p.add_argument("--flight-n", type=int, default=400, help="new creatives in each wave")
    p.add_argument("--process-s", type=float, default=8, help="the registry's typical job time (for the printout)")
    p.add_argument("--refresh-s", type=float, default=60, help="the slower verdict refresh to compare against")
    p.add_argument("--join-at", type=float, default=0, help="add a node this many seconds in (0 = no late node)")
    asyncio.run(main(p.parse_args()))
