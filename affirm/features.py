"""The feature platform's side of the lane: aggregates into the online store, and the version check at checkout.

Two readers, each reading every partition of its topic directly on connections of its own (no consumer group:
a group reader visits one partition per poll, so with 16 partitions a record waited for its turn):
  * user-aggregates (the lane): runs Affirm's one comparison, compare(stored, incoming), and stores the
    aggregate only when it is first, newer or a schema up-version; older and duplicate deliveries are
    counted and dropped. Change-to-store latency is timed here.
  * checkout-reads: plays the decision-time read. It asks the same comparison from the other direction:
    compare(what the store held when the checkout started, the system of record's counter). Duplicate
    means the stored value is current and is served; newer means the counter has moved and the expensive
    recompute runs; first means nothing is stored and the caller falls back to live aggregation.

Prints change-to-store p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, the write-path
outcomes, and the business numbers: of the checkouts that came within --recent-s of the customer's last
loan event, how many found the stored value current, needed a recompute or found nothing stored.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import (AGGREGATES, CHECKOUTS, DEFAULT_URL, DUPLICATE, FIRST, NEWER, OLDER, PARTITIONS, RECENT_S,
                    SCHEMA_UP, STREAM, Window, compare, ensure_topics, load)

KEEP = 4                                    # versions kept per customer, to answer "what was stored at time t"


async def main(a):
    async def own_client():                 # one connection per reader, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), AGGREGATES, CHECKOUTS)

    store = {}                              # customer -> the stored aggregate (the online store's row)
    versions = {}                           # customer -> [(applied ns, counter, schema)], newest last
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays aggregates made before it joined
    gc.freeze()

    async def on_aggregate(m):
        e = load(m.payload())
        u = e["u"]
        hist = versions.get(u)
        outcome = compare(hist and hist[-1][1:], (e["v"], e["schema"]))
        stats[outcome] += 1
        if outcome in (OLDER, DUPLICATE):
            return                          # delivered again or out of order: the stored value stays
        now = time.time_ns()
        store[u] = e
        if hist is None:
            hist = versions[u] = []
        hist.append((now, e["v"], e["schema"]))
        if len(hist) > KEEP:
            del hist[0]
        if e.get("snap"):
            return                          # the start-up snapshot is not a live change
        age = (now - e["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest change seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    def stored_at(u, t_ns):
        # (counter, schema) the store held at t_ns, or None if nothing was stored yet.
        hist = versions.get(u)
        if not hist:
            return None
        for applied, v, schema in reversed(hist):
            if applied <= t_ns:
                return v, schema
        return (-1, hist[0][2]) if len(hist) == KEEP else None   # older than the kept versions: behind

    async def on_checkout(m):
        c = load(m.payload())
        if not caught_up[0] or time.time_ns() - c["ns"] > 5e9:
            stats["held"] += 1              # replayed history or a checkout from before this reader ran
            return
        # The read started at c["ns"]. Same comparison as the write path, asked from the other direction.
        # (Both clocks are wall clocks; across machines, keep them NTP-synced.)
        outcome = compare(stored_at(c["u"], c["ns"]), (c["v"], c["schema"]))
        result = "current" if outcome in (DUPLICATE, OLDER) else "unavailable" if outcome == FIRST else "recompute"
        recent = c["last_ns"] and (c["ns"] - c["last_ns"]) / 1e9 < a.recent_s
        group = "recent" if recent else "other"
        stats[f"{group}"] += 1
        stats[f"{group}_{result}"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of changes arrived inside the budget; partitions interleave,
                # so one fresh change alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier changes this second", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>6,} changes/s  change-to-store p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  customers stored {len(store):,}  "
                  f"write path: first {stats[FIRST]:,} newer {stats[NEWER]:,} schema-up {stats[SCHEMA_UP]:,} "
                  f"duplicate {stats[DUPLICATE]:,} older {stats[OLDER]:,}", flush=True)
            n, o = stats["recent"], stats["other"]
            pct = lambda k, d: 100 * stats[k] / d if d else 0
            print(f"[checkout] reads {n + o:,}  within {a.recent_s:g} s of a loan event {n:,}: current "
                  f"{pct('recent_current', n):.2f}%  recompute {pct('recent_recompute', n):.2f}%  unavailable "
                  f"{pct('recent_unavailable', n):.2f}%  |  no recent change {o:,}: current {pct('other_current', o):.2f}%  "
                  f"held {stats['held']:,}", flush=True)
            stats.clear()

    async def read(topic, handler, pids):
        # Each partition in turn, from the start of the log (the store is rebuilt from it), then from this
        # reader's own position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{topic}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, topic, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        await handler(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def readers(topic, handler):
        parts = list(range(PARTITIONS[topic]))
        return [read(topic, handler, parts[r::a.readers]) for r in range(a.readers)]

    await asyncio.gather(*readers(AGGREGATES, on_aggregate), *readers(CHECKOUTS, on_checkout), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="features")
    p.add_argument("--budget-ms", type=int, default=50, help="change-to-store p99 budget for the lane")
    p.add_argument("--recent-s", type=float, default=RECENT_S, help="a checkout this soon after a loan event counts as recent")
    p.add_argument("--readers", type=int, default=2, help="connections reading each topic's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
