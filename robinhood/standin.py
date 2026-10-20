"""A stand-in authorizer: decides card authorizations from its own cache of every card account, fed by the lane.

Each process is one stand-in replica; run several. Every replica reads every partition of card-state directly
(its own offsets, no consumer group, nothing committed) on its own connection, so each one holds the latest
spendable balance and card status of every account (the "push" model). On start it rebuilds that cache from the
log before it decides anything.

Authorization requests arrive on auth-requests, shared among the replicas as one consumer group. Each is decided
from the cache: approve if the card is active and the balance covers the amount. A request older than --expire-s
is not decided (the processor has timed out and declined it). Decisions go to standin-decisions from a separate
task once every 10 ms, so the read loops never wait on a send. For comparison each replica also keeps the same
cache held --lag seconds behind and records what it would have decided; ledger.py checks both against the
primary.

Prints change-to-cache latency every second (p50, p99, p99.9) with PASS or FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DECISIONS, DEFAULT_URL, PARTITIONS, REQUESTS, STATE, STREAM, Window, connect, decide,
                    ensure_topics, load, msg)


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), STATE, REQUESTS, DECISIONS)
    reader, writer = await connect(a.url), await connect(a.url)       # one connection each way
    P = PARTITIONS[STATE]
    start_ns = time.time_ns()

    cache = {}                              # account -> (seq, spendable cents, card status), newest applied
    behind = {}                             # the same, --lag seconds behind
    delayed = deque()                       # changes waiting to reach the lagged cache
    outbox = []                             # decisions waiting for the sender task
    lat, st, run = Window(), defaultdict(int), defaultdict(int)
    caught = [None]

    def apply(c):
        k = c["k"]
        if c["seq"] <= cache.get(k, (0,))[0]:
            run["repeats"] += 1             # older or repeated change (replay, at-least-once): newest state wins
            return
        cache[k] = (c["seq"], c["avail"], c["status"])
        delayed.append((c["ns"], k, cache[k]))
        st["applied"] += 1

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
                        else:
                            run["replayed"] += 1
                        apply(c)
                if len(ms) == a.batch:
                    more = True             # a rebuild or a burst: read on at once
                elif p not in done:
                    done.add(p)             # this partition has been read to the end of the log once
                    if len(done) == P:
                        caught[0] = time.monotonic()
                        gc.freeze()         # keep the rebuilt cache out of the collector's way
                        print(f"[{a.name}] rebuilt the cache from the log in {(now - start_ns) / 1e9:.2f}s: "
                              f"{len(cache):,} card accounts, {run['replayed']:,} changes", flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def age_lagged_cache():
        while True:
            await asyncio.sleep(0.01)
            cutoff = time.time_ns() - int(a.lag * 1e9)
            while delayed and delayed[0][0] <= cutoff:
                _, k, v = delayed.popleft()
                behind[k] = v

    async def on_request(m):
        r = load(m.payload())
        now = time.time_ns()
        if now - r["ns"] > a.expire_s * 1e9:
            st["expired"] += 1              # the processor has already timed out on this one
            return
        k, amt = r["k"], r["amt"]
        seq, avail, status = cache.get(k, (0, 0, "unknown"))
        _, lavail, lstatus = behind.get(k, (0, 0, "unknown"))
        ok = decide(avail, status, amt)
        st["approved" if ok else "declined"] += 1
        outbox.append(SendMessage(msg(id=r["id"], k=k, seq=seq, ok=ok, lok=decide(lavail, lstatus, amt), by=me)))

    async def send_decisions():
        while True:
            await asyncio.sleep(0.01)
            if outbox:
                out, outbox[:] = list(outbox), []
                await writer.send_messages(STREAM, DECISIONS, Partitioning.balanced(), out)

    async def take_requests():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never decide before the cache is rebuilt
        group = await (await connect(a.url)).consumer_group(
            "standin", STREAM, REQUESTS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
            auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))
        await group.consume_messages(on_request, asyncio.Event())

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the cache from the log: {run['replayed']:,} changes so far", flush=True)
                lat.flush()
                st.clear()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>6,} changes/s  change-to-cache p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  "
                  f"p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  decided approved {st['approved']:,} "
                  f"declined {st['declined']:,}  expired {st['expired']:,}  cache {len(cache):,} accounts  "
                  f"read pass max {st['gap_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(read_lane(), age_lagged_cache(), take_requests(), send_decisions(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="standin-1")
    p.add_argument("--budget-ms", type=float, default=50, help="change-to-cache p99 budget")
    p.add_argument("--lag", type=float, default=0.5, help="seconds behind, for the comparison cache")
    p.add_argument("--expire-s", type=float, default=2.0, help="the processor's response deadline")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
