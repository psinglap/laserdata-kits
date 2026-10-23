"""A Zone Deals seat picker: picks the listing for every fan who buys a zone, from its own pool of every zone.

"You pick the Zone. We pick the seat." Each process is one picker; run several. Every picker reads every
partition of zone-listings directly (its own offsets, no consumer group, nothing committed) on its own
connection, so each one holds the latest state of every listing in every zone: tickets left, price, the seller's
floor. On start it rebuilds that pool from the log before it takes a single order.

Zone Deals orders arrive on zone-orders, keyed by event:zone. Each picker owns a share of its partitions (--shard
of --shards) and reads them directly too, so a zone's orders are picked by one picker; on start it takes orders
from that moment on, since older ones have timed out. For each order it picks, in that zone, the cheapest listing
with enough tickets left at or under the Zone Deal price the fan paid. Tickets this picker has picked that
checkout has not reported yet count against a listing's tickets left. Picks go to zone-picks from a separate task
every 5 ms, so the read loops never wait on a send.

For comparison, each picker also keeps a copy of its pool refreshed every --refresh-s seconds and records the
listing it would have picked from that copy; listings.py checks both picks against the listings.

Prints change-to-pool latency every second (p50, p99, p99.9) with PASS or FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, LISTINGS, ORDERS, PARTITIONS, PICKS, STREAM, Window, connect, ensure_topics, load, msg


class Pool:
    """The latest state of every listing, and the listings with tickets left in each zone."""

    def __init__(self):
        self.lst = {}                    # listing -> [seq, zone, qty, price, zone qty processed]
        self.zone = defaultdict(set)     # zone -> listings with tickets left

    def apply(self, c) -> bool:
        k = c["k"]
        v = self.lst.get(k)
        if v is not None and c["seq"] <= v[0]:
            return False                 # older or repeated change (replay, at-least-once): newest state wins
        self.lst[k] = [c["seq"], c["z"], c["qty"], c["price"], c["zq"]]
        (self.zone[c["z"]].add if c["qty"] > 0 else self.zone[c["z"]].discard)(k)
        return True

    def copy(self):
        p = Pool()
        p.lst = {k: v[:] for k, v in self.lst.items()}
        p.zone = defaultdict(set, {z: set(s) for z, s in self.zone.items()})
        return p

    def pick(self, z, qty, paid, sent):
        best, best_price = "", None
        for k in self.zone.get(z, ()):
            v = self.lst[k]
            left = v[2] - max(0, sent.get(k, 0) - v[4])          # minus tickets picked that checkout hasn't reported
            if left < qty or v[3] > paid:
                continue
            if best_price is None or v[3] < best_price:
                best, best_price = k, v[3]
        return best


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), LISTINGS, ORDERS, PICKS)
    reader, writer = await connect(a.url), await connect(a.url)       # one connection each way
    P = PARTITIONS[LISTINGS]
    start_ns = time.time_ns()

    pool, stale = Pool(), [Pool()]
    sent = defaultdict(int)              # listing -> tickets this picker has picked on it
    outbox = []                          # picks waiting for the sender task
    lat, st, run = Window(), defaultdict(int), defaultdict(int)
    caught = [None]

    async def read_lane():
        who = {p: Consumer.Single(f"{me}-{p}") for p in range(P)}
        nxt, done = {}, set()
        last = time.monotonic()
        while True:
            now_m = time.monotonic()
            st["gap_ms"] = max(st["gap_ms"], 1000 * (now_m - last))    # longest pass over all partitions
            last, more = now_m, False
            for p in range(P):
                ms = await reader.poll_messages(STREAM, LISTINGS, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                                else PollingStrategy.First())
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        c = load(m.payload())
                        if caught[0] is not None and c["seq"] > 1 and c["ns"] >= start_ns:
                            lat.add((now - c["ns"]) / 1e6)      # a live change, not the rebuild
                            st["changes"] += 1
                        else:
                            run["replayed"] += 1
                        if not pool.apply(c):
                            run["repeats"] += 1
                if len(ms) == a.batch:
                    more = True             # a rebuild or a burst: read on at once
                elif p not in done:
                    done.add(p)             # this partition has been read to the end of the log once
                    if len(done) == P:
                        caught[0] = time.monotonic()
                        stale[0] = pool.copy()
                        gc.freeze()         # keep the rebuilt pool out of the collector's way
                        live = sum(1 for s in pool.zone.values() for _ in s)
                        print(f"[{a.name}] rebuilt the pool from the log in {(now - start_ns) / 1e9:.2f}s: "
                              f"{len(pool.lst):,} listings in {len(pool.zone):,} zones, {live:,} with tickets left, "
                              f"{run['replayed']:,} changes replayed", flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def refresh_copy():
        # The comparison pool: the same state, copied every --refresh-s seconds (a periodic inventory sync).
        while caught[0] is None:
            await asyncio.sleep(0.05)
        due = time.monotonic()
        while True:
            due += a.refresh_s
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            stale[0] = pool.copy()

    def on_order(m):
        o = load(m.payload())
        if time.time_ns() - o["ns"] > a.expire_s * 1e9:
            st["expired"] += 1              # the fan's checkout has already timed out
            return
        k = pool.pick(o["z"], o["qty"], o["paid"], sent)
        alt = stale[0].pick(o["z"], o["qty"], o["paid"], sent)
        if k:
            sent[k] += o["qty"]
            st["picked"] += 1
        else:
            st["none"] += 1
        outbox.append(SendMessage(msg(id=o["id"], k=k, alt=alt, by=me)))

    async def send_picks():
        while True:
            await asyncio.sleep(0.005)
            if outbox:
                out, outbox[:] = list(outbox), []
                await writer.send_messages(STREAM, PICKS, Partitioning.balanced(), out)

    async def take_orders():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never pick before the pool is rebuilt
        orr = await connect(a.url)          # its own connection, so orders never queue behind the lane
        mine = [p for p in range(PARTITIONS[ORDERS]) if p % a.shards == a.shard]
        who = {p: Consumer.Single(f"{me}-orders-{p}") for p in mine}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)     # orders from now on
        nxt = {}
        while True:
            more = False
            for p in mine:
                ms = await orr.poll_messages(STREAM, ORDERS, consumer=who[p], partition_id=p, count=a.batch,
                                             auto_commit=False,
                                             polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_order(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the pool from the log: {run['replayed']:,} changes so far", flush=True)
                lat.flush()
                st.clear()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {st['changes']:>6,} changes/s  change-to-pool p50 {r['p50']:5.1f}ms  "
                  f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"zone orders picked {st['picked']:,}  no seat {st['none']:,}  expired {st['expired']:,}  "
                  f"pool {len(pool.lst):,} listings  read pass max {st['gap_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(read_lane(), refresh_copy(), take_orders(), send_picks(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="zp-1")
    p.add_argument("--budget-ms", type=float, default=50, help="change-to-pool p99 budget")
    p.add_argument("--shard", type=int, default=0, help="this picker's share of the zones: 0 .. shards-1")
    p.add_argument("--shards", type=int, default=1, help="how many pickers share the zones")
    p.add_argument("--refresh-s", type=float, default=3, help="refresh period of the comparison pool")
    p.add_argument("--expire-s", type=float, default=2.0, help="an order older than this has timed out")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
