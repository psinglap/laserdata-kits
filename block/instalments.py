"""A subscriber: the instalment collections service, with its own cache of every customer's payment token, fed by
the lane.

Each process is one subscriber; run several (collections, refunds, anything that opted in). Every subscriber reads
every partition of payment-token-changes directly (its own offsets, no consumer group, nothing committed) on its
own connection. On start it rebuilds its cache from the log before it charges anything.

Every record is checked before it is applied: the HMAC signature must match (a record that fails is counted and
dropped), and the body must hold no card number (any 13-19 digit run that passes the Luhn check is counted). A
record whose sequence is not newer than the cached one changes nothing, so replays and repeats are safe.

The service charges instalments as they fall due (--charges per second, spread over all customers), each with the
token in its cache, and sends each charge to instalment-charges with the token version it used, from a separate
task every 10 ms so the read loop never waits on a send. For comparison it also keeps the same cache fed --lag
seconds behind and records which version that cache would have used; token_jobs.py checks both against the vault.

Prints change-to-cache latency every second (p50, p99, p99.9) with PASS or FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (CHARGES, DEFAULT_URL, PARTITIONS, STREAM, TOKENS, Window, connect, ensure_topics,
                    load, msg, strings_with_card_numbers, verify)


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), TOKENS, CHARGES)
    reader, writer = await connect(a.url), await connect(a.url)       # one connection each way
    P = PARTITIONS[TOKENS]
    start_ns = time.time_ns()
    rng = random.Random()

    cache = {}                              # customer -> (seq, token id, status), newest applied
    behind = {}                             # the same, --lag seconds behind
    delayed = deque()                       # changes waiting to reach the lagged cache
    outbox = []                             # charges waiting for the sender task
    lat, st, run = Window(), defaultdict(int), defaultdict(int)
    caught = [None]
    keys = []

    def apply(c):
        k = c["k"]
        if c["seq"] <= cache.get(k, (0,))[0]:
            run["repeats"] += 1             # older or repeated change (replay, at-least-once): newest token wins
            return
        if k not in cache:
            keys.append(k)
        cache[k] = (c["seq"], c["token"], c["status"])
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
                ms = await reader.poll_messages(STREAM, TOKENS, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                                else PollingStrategy.First())
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        body = verify(m.payload())
                        if body is None:
                            st["bad_sig"] += 1      # not signed by the payments platform, or changed on the way
                            continue
                        c = load(body)
                        if strings_with_card_numbers(c):
                            st["pan_seen"] += 1     # the producer's guard let a card number through
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
                        print(f"[{a.name}] rebuilt the token cache from the log in {(now - start_ns) / 1e9:.2f}s: "
                              f"{len(cache):,} customers, {run['replayed']:,} changes", flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def age_lagged_cache():
        while True:
            await asyncio.sleep(0.01)
            cutoff = time.time_ns() - int(a.lag * 1e9)
            while delayed and delayed[0][0] <= cutoff:
                _, k, v = delayed.popleft()
                behind[k] = v

    async def charge_instalments():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never charge before the cache is rebuilt
        n, owed, due = 0, 0.0, time.monotonic()
        while True:
            owed += a.charges / 100
            for _ in range(int(owed)):
                k = rng.choice(keys)
                seq, tid, status = cache[k]
                if status != "active":
                    st["no_card"] += 1      # no usable card on file: the customer is asked to add one instead
                    continue
                n += 1
                lseq = behind.get(k, (1,))[0]
                st["charged"] += 1
                outbox.append(SendMessage(msg(id=f"{a.name}-{n}", k=k, seq=seq, token=tid, lseq=lseq,
                                              amount=rng.choice((2500, 3750, 5000, 12500)), by=me)))
            owed -= int(owed)
            due += 0.01                     # from the due time, not from now
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def send_charges():
        while True:
            await asyncio.sleep(0.01)
            if outbox:
                out, outbox[:] = list(outbox), []
                await writer.send_messages(STREAM, CHARGES, Partitioning.balanced(), out)

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the token cache from the log: {run['replayed']:,} changes so far", flush=True)
                lat.flush()
                st.clear()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>6,} changes/s  change-to-cache p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  "
                  f"p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  instalments charged {st['charged']:,}  "
                  f"no usable card {st['no_card']:,}  bad signatures {st['bad_sig']}  card numbers seen {st['pan_seen']}  "
                  f"cache {len(cache):,} customers  read pass max {st['gap_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(read_lane(), age_lagged_cache(), charge_instalments(), send_charges(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="collections-1")
    p.add_argument("--budget-ms", type=float, default=50, help="change-to-cache p99 budget")
    p.add_argument("--charges", type=float, default=2000, help="instalment charges per second")
    p.add_argument("--lag", type=float, default=5.0, help="seconds behind, for the comparison cache")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
