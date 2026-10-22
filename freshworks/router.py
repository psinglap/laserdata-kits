"""A routing node: picks an agent for every new ticket and chat, from its own view of every agent.

Each process is one routing node; run several. Every node reads every partition of agent-state directly (its own
offsets, no consumer group, nothing committed) on its own connection, so each one holds the latest state of every
agent: group, status, tickets and chats against capacity, and whether the agent is on a call. On start it rebuilds
that view from the log before it routes a single inquiry.

New tickets and chats arrive on inquiries, keyed by group. Each node owns a share of its partitions (--shard of
--shards) and reads them directly too, so a group's inquiries are routed by one node; on start it takes inquiries
from that moment on. Each inquiry is routed the way Freshdesk documents load-based automatic assignment: only
agents who are available, not on a call and under their capacity for that channel are eligible; the one with the
most room (lowest load against capacity) wins, and the last assignment time breaks ties. Assignments this node
sent that the product has not reported yet count against the agent's capacity. Assignments go to assignments
from a separate task every 5 ms, so the read loops never wait on a send.

For comparison, each node also keeps a copy of its view refreshed every --refresh-s seconds and records the agent
it would have picked from that copy; agents.py checks both picks against the agents.

Prints change-to-view latency every second (p50, p99, p99.9) with PASS or FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (ASSIGNMENTS, DEFAULT_URL, INQUIRIES, PARTITIONS, STATE, STREAM, Window, connect, ensure_topics,
                    load, msg)


class View:
    """The latest state of every agent, and the agents who can take work, per group."""

    def __init__(self):
        self.ag = {}                     # agent -> [seq, group, status, tickets, ticket cap, chats, chat cap, call, seen]
        self.ready = defaultdict(set)    # group -> agents who are available and not on a call

    def apply(self, c) -> bool:
        k = c["k"]
        v = self.ag.get(k)
        if v is not None and c["seq"] <= v[0]:
            return False                 # older or repeated change (replay, at-least-once): newest state wins
        self.ag[k] = [c["seq"], c["g"], c["st"], c["t"], c["ct"], c["c"], c["cc"], c["call"], c["seen"]]
        if c["st"] == "available" and not c["call"]:
            self.ready[c["g"]].add(k)
        else:
            self.ready[c["g"]].discard(k)
        return True

    def copy(self):
        vw = View()
        vw.ag = {k: v[:] for k, v in self.ag.items()}
        vw.ready = defaultdict(set, {g: set(s) for g, s in self.ready.items()})
        return vw

    def pick(self, g, ch, sent, last):
        best, best_key = "", None
        for k in self.ready.get(g, ()):
            v = self.ag[k]
            load, cap = (v[3], v[4]) if ch == "ticket" else (v[5], v[6])
            load += max(0, sent.get(k, 0) - v[8])     # plus assignments the product has not reported yet
            if load >= cap:
                continue                 # at capacity: not eligible
            key = (load / cap, last.get(k, 0))        # most room first, then the longest since an assignment
            if best_key is None or key < best_key:
                best, best_key = k, key
        return best


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), STATE, INQUIRIES, ASSIGNMENTS)
    reader, writer = await connect(a.url), await connect(a.url)       # one connection each way
    P = PARTITIONS[STATE]
    start_ns = time.time_ns()

    view, stale = View(), [View()]
    sent = defaultdict(int)              # agent -> assignments this node sent
    last = {}                            # agent -> when this node last assigned to it
    outbox = []                          # assignments waiting for the sender task
    lat, st, run = Window(), defaultdict(int), defaultdict(int)
    caught = [None]

    async def read_lane():
        who = {p: Consumer.Single(f"{me}-{p}") for p in range(P)}
        nxt, done = {}, set()
        prev = time.monotonic()
        while True:
            now_m = time.monotonic()
            st["gap_ms"] = max(st["gap_ms"], 1000 * (now_m - prev))    # longest pass over all partitions
            prev, more = now_m, False
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
                        if not view.apply(c):
                            run["repeats"] += 1
                if len(ms) == a.batch:
                    more = True             # a rebuild or a burst: read on at once
                elif p not in done:
                    done.add(p)             # this partition has been read to the end of the log once
                    if len(done) == P:
                        caught[0] = time.monotonic()
                        stale[0] = view.copy()
                        gc.freeze()         # keep the rebuilt view out of the collector's way
                        avail = sum(1 for v in view.ag.values() if v[2] == "available")
                        print(f"[{a.name}] rebuilt the view from the log in {(now - start_ns) / 1e9:.2f}s: "
                              f"{len(view.ag):,} agents, {avail:,} available, "
                              f"{run['replayed']:,} changes replayed", flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def refresh_copy():
        # The comparison view: the same state, copied every --refresh-s seconds (a periodic reload).
        while caught[0] is None:
            await asyncio.sleep(0.05)
        due = time.monotonic()
        while True:
            due += a.refresh_s
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            stale[0] = view.copy()

    def on_inquiry(m):
        r = load(m.payload())
        if time.time_ns() - r["ns"] > a.expire_s * 1e9:
            st["expired"] += 1              # too old to route here
            return
        k = view.pick(r["g"], r["ch"], sent, last)
        alt = stale[0].pick(r["g"], r["ch"], sent, last)
        if k:
            sent[k] += 1
            last[k] = time.monotonic()
            st["routed"] += 1
        else:
            st["none"] += 1
        outbox.append(SendMessage(msg(id=r["id"], a=k, alt=alt, by=me)))

    async def send_assignments():
        while True:
            await asyncio.sleep(0.005)
            if outbox:
                out, outbox[:] = list(outbox), []
                await writer.send_messages(STREAM, ASSIGNMENTS, Partitioning.balanced(), out)

    async def take_inquiries():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never route before the view is rebuilt
        ir = await connect(a.url)           # its own connection, so inquiries never queue behind the lane
        mine = [p for p in range(PARTITIONS[INQUIRIES]) if p % a.shards == a.shard]
        who = {p: Consumer.Single(f"{me}-inq-{p}") for p in mine}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)     # inquiries from now on
        nxt = {}
        while True:
            more = False
            for p in mine:
                ms = await ir.poll_messages(STREAM, INQUIRIES, consumer=who[p], partition_id=p, count=a.batch,
                                            auto_commit=False,
                                            polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_inquiry(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the view from the log: {run['replayed']:,} changes so far", flush=True)
                lat.flush()
                st.clear()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {st['changes']:>6,} changes/s  change-to-view p50 {r['p50']:5.1f}ms  "
                  f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"routed {st['routed']:,}  no agent {st['none']:,}  expired {st['expired']:,}  "
                  f"view {len(view.ag):,} agents  read pass max {st['gap_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(read_lane(), refresh_copy(), take_inquiries(), send_assignments(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="route-1")
    p.add_argument("--budget-ms", type=float, default=50, help="change-to-view p99 budget")
    p.add_argument("--shard", type=int, default=0, help="this node's share of the groups: 0 .. shards-1")
    p.add_argument("--shards", type=int, default=1, help="how many routing nodes share the groups")
    p.add_argument("--refresh-s", type=float, default=3, help="refresh period of the comparison view")
    p.add_argument("--expire-s", type=float, default=2.0, help="an inquiry older than this is not routed here")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
