"""A bidder-facing edge node: holds the live state of every sale lane and plays the members connected to it.

Each process is one edge node (the servers that hold members' bidding screens); run several. Every edge reads
every partition of lot-state directly (its own offsets, no consumer group, nothing committed) on its own
connection, so each one holds the latest state of every lane: the lot on the block, the high bid, the high
bidder's flag, when the countdown runs out. On start it rebuilds that state from the log before it takes a
single bid.

Its members watch every lane (a member can follow several auctions at once on one screen). While the price on a
lot is under what the room will pay for it, they bid the next increment over the high bid their screen shows;
the closer the price gets, the fewer bid. Bids go to live-bids from a separate task every 5 ms, so the read loop
never waits on a send.

For comparison, each edge also keeps a copy of the lane states refreshed every --refresh-s seconds, and every
bid carries the bid the same member would have placed from that screen; auction_block.py judges both.

Prints state-to-edge latency every second (p50, p99, p99.9) with PASS or FAIL against --budget-ms, and the time
from a member's bid to the moment their flag shows as high bidder (bid-to-flag).
"""
import argparse
import asyncio
import gc
import os
import random
import time
import zlib
from collections import defaultdict

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (BIDS, DEFAULT_URL, LOT_STATE, PARTITIONS, REGIONS, STREAM, Window, connect, ensure_topics,
                    increment, load, msg, part_of)

SEQ, LOT, ST, HIGH, ERV, DL = range(6)


def room_pays(lot: str, erv: int) -> float:
    # What the room will pay for this lot, as a share of its estimated retail value (illustrative, fixed per lot).
    return erv * (0.2 + 0.5 * (zlib.crc32(lot.encode()) % 1000) / 1000)


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), LOT_STATE, BIDS)
    reader, writer = await connect(a.url), await connect(a.url)       # one connection each way
    P = PARTITIONS[LOT_STATE]
    start_ns = time.time_ns()
    rng = random.Random(os.getpid())

    lanes, stale = {}, [{}]               # lane -> [seq, lot, status, high, erv, deadline ns]
    mine = {}                             # bid id -> created ns, for bids from this edge's members
    outbox = []                           # bids waiting for the sender task
    lat, flag, st, run = Window(), Window(), defaultdict(int), defaultdict(int)
    caught = [None]
    ids = iter(range(1, 1 << 62))

    def apply(c) -> bool:
        v = lanes.get(c["k"])
        if v is not None and c["seq"] <= v[SEQ]:
            return False                  # older or repeated change (replay, at-least-once): newest state wins
        lanes[c["k"]] = [c["seq"], c["lot"], c["st"], c["high"], c["erv"], c["dl"]]
        return True

    async def read_lane():
        who = {p: Consumer.Single(f"{me}-{p}") for p in range(P)}
        nxt, done = {}, set()
        last = time.monotonic()
        while True:
            now_m = time.monotonic()
            st["gap_ms"] = max(st["gap_ms"], 1000 * (now_m - last))    # longest pass over all partitions
            last, more = now_m, False
            for p in range(P):
                ms = await reader.poll_messages(STREAM, LOT_STATE, consumer=who[p], partition_id=p, count=a.batch,
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
                            t0 = mine.pop(c["bid"], None) if c["bid"] else None
                            if t0:
                                flag.add((now - t0) / 1e6)      # this member's flag is now on the screen
                        else:
                            run["replayed"] += 1
                        apply(c)
                if len(ms) == a.batch:
                    more = True             # a rebuild or a burst: read on at once
                elif p not in done:
                    done.add(p)             # this partition has been read to the end of the log once
                    if len(done) == P:
                        caught[0] = time.monotonic()
                        stale[0] = {k: v[:] for k, v in lanes.items()}
                        gc.freeze()         # keep the rebuilt state out of the collector's way
                        live = sum(1 for v in lanes.values() if v[ST] == "open")
                        print(f"[{a.name}] rebuilt every lane from the log in {(now - start_ns) / 1e9:.2f}s: "
                              f"{len(lanes):,} lanes, {live:,} lots open, {run['replayed']:,} changes replayed",
                              flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def refresh_copy():
        # The comparison screen: the same state, copied every --refresh-s seconds.
        while caught[0] is None:
            await asyncio.sleep(0.05)
        due = time.monotonic()
        while True:
            due += a.refresh_s
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            stale[0] = {k: v[:] for k, v in lanes.items()}

    async def members():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never bid before every lane is rebuilt
        tick = a.tick_ms / 1000
        per_tick = a.bid_rate / a.edges * tick
        due = time.monotonic()
        while True:
            now = time.time_ns()
            for k, v in lanes.items():
                if v[ST] != "open" or now >= v[DL]:
                    continue                # sold, or the countdown has run out on the screen
                cap = room_pays(v[LOT], v[ERV])
                amt = v[HIGH] + increment(v[HIGH])
                if amt > cap or rng.random() >= per_tick * (1 - v[HIGH] / cap) ** 0.5:
                    continue
                bid_id = f"{a.name}-{next(ids)}"
                s = stale[0].get(k)
                alt = dict(alt_lot=s[LOT], alt=s[HIGH] + increment(s[HIGH])) if s and s[ST] == "open" else {}
                mine[bid_id] = now
                outbox.append((k, SendMessage(msg(id=bid_id, k=k, lot=v[LOT], amt=amt, r=rng.choice(REGIONS),
                                                  edge=a.name, ns=now, **alt))))
                st["bids"] += 1
            due += tick
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def send_bids():
        while True:
            await asyncio.sleep(0.005)
            if outbox:
                items, outbox[:] = list(outbox), []
                by_p = defaultdict(list)    # one batch per partition, all sent at once
                for k, m in items:
                    by_p[part_of(k, PARTITIONS[BIDS])].append(m)
                await asyncio.gather(*(writer.send_messages(STREAM, BIDS, Partitioning.partition_id(p), ms)
                                       for p, ms in by_p.items()))

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding every lane from the log: {run['replayed']:,} changes so far", flush=True)
                lat.flush()
                st.clear()
                continue
            cutoff = time.time_ns() - 5_000_000_000
            for b in [b for b, t in mine.items() if t < cutoff]:
                del mine[b]                 # not accepted (outbid or after the hammer): no flag to wait for
            r, f = lat.flush(), flag.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            open_now = sum(1 for v in lanes.values() if v[ST] == "open")
            print(f"[{a.name}] {st['changes']:>6,} state changes/s  state-to-edge p50 {r['p50']:5.1f}ms  "
                  f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"bids sent {st['bids']:,}  bid-to-flag p50 {f['p50']:.1f}ms p99 {f['p99']:.1f}ms  "
                  f"lots open {open_now:,}  read pass max {st['gap_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(read_lane(), refresh_copy(), members(), send_bids(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="edge-1")
    p.add_argument("--edges", type=int, default=1, help="how many edge nodes share the members")
    p.add_argument("--bid-rate", type=float, default=4, help="live bids per second on a lot while the price is low, all edges")
    p.add_argument("--budget-ms", type=float, default=50, help="state-to-edge p99 budget")
    p.add_argument("--refresh-s", type=float, default=1, help="refresh period of the comparison screen")
    p.add_argument("--tick-ms", type=float, default=10, help="how often members decide to bid")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
