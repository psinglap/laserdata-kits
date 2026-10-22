"""A matchmaker: picks a game server for every player who presses Play, from its own index of every server.

Each process is one matchmaker; run several. Every matchmaker reads every partition of server-state directly (its
own offsets, no consumer group, nothing committed) on its own connection, so each one holds the latest state of
every server: place, edge data center, capacity, players, open or shutting down. On start it rebuilds that index
from the log before it takes a single join.

Join requests arrive on join-requests, keyed by place. Each matchmaker owns a share of its partitions (--shard of
--shards) and reads them directly too, so a place's joins are matched by one matchmaker; on start it takes joins
from that moment on, since older ones have timed out. Each request is matched the way Roblox's documented
default matchmaking works: drop servers that are full or shutting down, score the rest (here two of the default
signals: Latency, the player's own edge data center first, weight 3; Occupancy, fuller servers first, weight 2),
pick the highest. Placements this matchmaker sent that a server has not reported yet count against its free
slots. Placements go to placements from a separate task every 5 ms, so the read loops never wait on a send.

For comparison, each matchmaker also keeps a copy of its index refreshed every --refresh-s seconds and records
the server it would have picked from that copy; fleet.py checks both picks against the servers.

Prints change-to-index latency every second (p50, p99, p99.9) with PASS or FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, JOINS, PARTITIONS, PLACEMENTS, STATE, STREAM, Window, connect, ensure_topics,
                    load, msg)

W_LATENCY, W_OCCUPANCY = 3.0, 2.0      # weights from Roblox's default matchmaking configuration


class Index:
    """The latest state of every server, and the open servers with free slots per place and data center."""

    def __init__(self):
        self.srv = {}                    # server -> [seq, place, dc, cap, players, status, seen]
        self.free = defaultdict(set)     # (place, dc) -> open servers with a free slot
        self.dcs = defaultdict(set)      # place -> data centers it runs in

    def apply(self, c) -> bool:
        k = c["k"]
        v = self.srv.get(k)
        if v is not None and c["seq"] <= v[0]:
            return False                 # older or repeated change (replay, at-least-once): newest state wins
        self.srv[k] = [c["seq"], c["place"], c["dc"], c["cap"], c["n"], c["st"], c["seen"]]
        pd = (c["place"], c["dc"])
        self.dcs[c["place"]].add(c["dc"])
        if c["st"] == "open" and c["n"] < c["cap"]:
            self.free[pd].add(k)
        else:
            self.free[pd].discard(k)
        return True

    def copy(self):
        ix = Index()
        ix.srv = {k: v[:] for k, v in self.srv.items()}
        ix.free = defaultdict(set, {pd: set(s) for pd, s in self.free.items()})
        ix.dcs = defaultdict(set, {p: set(s) for p, s in self.dcs.items()})
        return ix

    def pick(self, place, dc, sent):
        best, best_score = "", -1.0
        for d in [dc] + sorted(self.dcs.get(place, ()) - {dc}):
            lat = 1.0 if d == dc else 0.3
            for k in self.free.get((place, d), ()):
                v = self.srv[k]
                occ = v[4] + max(0, sent.get(k, 0) - v[6])       # plus placements the server has not reported
                if occ >= v[3]:
                    continue
                score = W_LATENCY * lat + W_OCCUPANCY * occ / v[3]
                if score > best_score:
                    best, best_score = k, score
            if best:
                return best              # a server in the player's own data center beats any farther one
        return best


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), STATE, JOINS, PLACEMENTS)
    reader, writer = await connect(a.url), await connect(a.url)       # one connection each way
    P = PARTITIONS[STATE]
    start_ns = time.time_ns()

    index, stale = Index(), [Index()]
    sent = defaultdict(int)              # server -> placements this matchmaker sent to it
    outbox = []                          # placements waiting for the sender task
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
                ms = await reader.poll_messages(STREAM, STATE, consumer=who[p], partition_id=p, count=a.batch,
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
                        if not index.apply(c):
                            run["repeats"] += 1
                if len(ms) == a.batch:
                    more = True             # a rebuild or a burst: read on at once
                elif p not in done:
                    done.add(p)             # this partition has been read to the end of the log once
                    if len(done) == P:
                        caught[0] = time.monotonic()
                        stale[0] = index.copy()
                        gc.freeze()         # keep the rebuilt index out of the collector's way
                        live = sum(1 for v in index.srv.values() if v[5] != "closed")
                        print(f"[{a.name}] rebuilt the index from the log in {(now - start_ns) / 1e9:.2f}s: "
                              f"{live:,} servers in {len(index.dcs):,} places, {run['replayed']:,} changes replayed",
                              flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def refresh_copy():
        # The comparison index: the same state, copied every --refresh-s seconds (a periodic reindex).
        while caught[0] is None:
            await asyncio.sleep(0.05)
        due = time.monotonic()
        while True:
            due += a.refresh_s
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            stale[0] = index.copy()

    def on_request(m):
        r = load(m.payload())
        if time.time_ns() - r["ns"] > a.expire_s * 1e9:
            st["expired"] += 1              # the player's join has already timed out
            return
        s = index.pick(r["place"], r["dc"], sent)
        alt = stale[0].pick(r["place"], r["dc"], sent)
        if s:
            sent[s] += 1
            st["matched"] += 1
        else:
            st["none"] += 1
        outbox.append(SendMessage(msg(id=r["id"], s=s, alt=alt, by=me)))

    async def send_placements():
        while True:
            await asyncio.sleep(0.005)
            if outbox:
                out, outbox[:] = list(outbox), []
                await writer.send_messages(STREAM, PLACEMENTS, Partitioning.balanced(), out)

    async def take_joins():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never match before the index is rebuilt
        jr = await connect(a.url)           # its own connection, so joins never queue behind the lane
        mine = [p for p in range(PARTITIONS[JOINS]) if p % a.shards == a.shard]
        who = {p: Consumer.Single(f"{me}-joins-{p}") for p in mine}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)     # joins from now on
        nxt = {}
        while True:
            more = False
            for p in mine:
                ms = await jr.poll_messages(STREAM, JOINS, consumer=who[p], partition_id=p, count=a.batch,
                                            auto_commit=False,
                                            polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_request(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the index from the log: {run['replayed']:,} changes so far", flush=True)
                lat.flush()
                st.clear()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {st['changes']:>6,} changes/s  change-to-index p50 {r['p50']:5.1f}ms  "
                  f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"joins matched {st['matched']:,}  no server {st['none']:,}  expired {st['expired']:,}  "
                  f"index {len(index.srv):,} servers  read pass max {st['gap_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(read_lane(), refresh_copy(), take_joins(), send_placements(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="mm-1")
    p.add_argument("--budget-ms", type=float, default=50, help="change-to-index p99 budget")
    p.add_argument("--shard", type=int, default=0, help="this matchmaker's share of the places: 0 .. shards-1")
    p.add_argument("--shards", type=int, default=1, help="how many matchmakers share the places")
    p.add_argument("--refresh-s", type=float, default=3, help="refresh period of the comparison index")
    p.add_argument("--expire-s", type=float, default=2.0, help="a join request older than this has timed out")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
