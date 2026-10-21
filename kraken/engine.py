"""The spot trading engine's output: every order event for every account, written once to the execution report lane.

Each event is an execution report keyed by account, with a per-account sequence number and the order's resulting
state, using the exec_type values of Kraken's WebSocket v2 executions channel:
  * new: an order is live in the engine;
  * trade: the order got a partial fill; filled: it is fully filled and closed;
  * amended: the client changed the limit price; canceled: the client cancelled it.

Writers are on their own connections, each owning some partitions, and write every --tick-ms; an event created
at some point in the last tick waits for that write, and the wait counts in the latency the gateways measure.
--burst multiplies the event rate for --burst-for seconds (a volatile market: Kraken's April 2023 post shows
request throughput rising from 250k to over 1mm requests a minute, and jitter held under 30 ms through the
FTX-collapse week).
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import (DEFAULT_URL, PAIRS, PARTITIONS, REPORTS, STREAM, Window, account, connect, ensure_topics, msg,
                    order_id, part_of)

WRITERS = PARTITIONS[REPORTS]          # one writer connection per partition


async def main(a):
    admin = await connect(a.url)
    await ensure_topics(admin, REPORTS)
    P = PARTITIONS[REPORTS]
    writers = [await connect(a.url) for _ in range(WRITERS)]   # partition p is written by writers[p % WRITERS]
    keys = [account(i) for i in range(a.accounts)]
    rng = random.Random(11)
    seq = defaultdict(int)
    open_orders = defaultdict(dict)          # account -> order id -> [pair, side, qty, filled, price]
    n_orders = [0]
    acks, st, t0 = Window(), defaultdict(int), time.monotonic()

    def report(k, oid, o, exec_type, status, waited_ns):
        seq[k] += 1
        pair, side, qty, filled, px = o
        st[exec_type] += 1
        return k, SendMessage(msg(k=k, seq=seq[k], order_id=oid, symbol=pair, side=side, exec_type=exec_type,
                                  order_status=status, order_qty=qty, cum_qty=filled, limit_price=px,
                                  ns=time.time_ns() - waited_ns))

    def one_event(waited_ns):
        # A few accounts (market makers, bots on the API) trade far more than the long tail.
        k = keys[int(len(keys) * rng.random() ** 2)]
        mine = open_orders[k]
        r = rng.random()
        if not mine or r < 0.40:
            n_orders[0] += 1
            oid = order_id(n_orders[0])
            pair = PAIRS[int(len(PAIRS) * rng.random() ** 2)]
            o = [pair, rng.choice(("buy", "sell")), round(rng.uniform(0.01, 2.0), 4), 0.0,
                 round(rng.uniform(10, 70_000), 2)]
            if len(mine) >= 8:               # cancel the oldest first: open orders per account stay bounded
                old = next(iter(mine))
                st["evicted"] += 1
                return [report(k, old, mine.pop(old), "canceled", "canceled", waited_ns),
                        report(k, oid, mine.setdefault(oid, o), "new", "new", waited_ns)]
            mine[oid] = o
            return [report(k, oid, o, "new", "new", waited_ns)]
        oid = rng.choice(list(mine))
        o = mine[oid]
        if r < 0.75:                         # a fill: partial, or the rest of the order
            part = round(min(o[2] - o[3], o[2] * rng.choice((0.25, 0.5, 1.0))), 4)
            o[3] = round(o[3] + part, 4)
            if o[3] >= o[2]:
                del mine[oid]
                return [report(k, oid, o, "filled", "filled", waited_ns)]
            return [report(k, oid, o, "trade", "partially_filled", waited_ns)]
        if r < 0.85:
            o[4] = round(o[4] * rng.uniform(0.995, 1.005), 2)
            return [report(k, oid, o, "amended", "partially_filled" if o[3] else "new", waited_ns)]
        del mine[oid]
        return [report(k, oid, o, "canceled", "canceled", waited_ns)]

    async def send(items):
        out = defaultdict(list)              # one batch per partition: an account's reports stay in order
        for k, m in items:
            out[part_of(k, P)].append(m)
        s0 = time.perf_counter()
        await asyncio.gather(*(writers[p % WRITERS].send_messages(STREAM, REPORTS, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))
        acks.add((time.perf_counter() - s0) * 1e3)

    async def run():
        gc.freeze()                          # keep start-up tables out of the collector's way
        nonlocal t0
        t0 = due = time.monotonic()
        owed = 0.0
        while True:
            el = time.monotonic() - t0
            x = a.burst if a.burst_at and a.burst_at <= el < a.burst_at + a.burst_for else 1
            owed += a.rate * x * a.tick_ms / 1000
            items = []
            for _ in range(int(owed)):
                items += one_event(int(rng.random() * a.tick_ms * 1e6))
            owed -= int(owed)
            st["late_ms"] = max(st["late_ms"], 1000 * (time.monotonic() - due))
            if items:
                await send(items)
                st["events"] += len(items)
            st["burst"] = int(x > 1)
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def show():
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[engine] {st['events']:>6,} reports/s{' (burst %gx)' % a.burst if st['burst'] else ''}  "
                  f"new {st['new']:,}  trade {st['trade']:,}  filled {st['filled']:,}  amended {st['amended']:,}  "
                  f"canceled {st['canceled']:,}  write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms  "
                  f"tick late max {st['late_ms']:.0f}ms", flush=True)
            st.clear()

    print(f"[engine] {a.accounts:,} trading accounts, {len(PAIRS)} pairs, {a.rate:,.0f} reports/s, "
          f"{P} partitions keyed by account", flush=True)
    await asyncio.gather(run(), show())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--accounts", type=int, default=200_000, help="accounts with orders on the book")
    p.add_argument("--rate", type=float, default=15_000, help="execution reports per second (all accounts)")
    p.add_argument("--tick-ms", type=float, default=2, help="the writers write once per this many ms")
    p.add_argument("--burst", type=float, default=1, help="rate multiplier in a volatile market")
    p.add_argument("--burst-at", type=float, default=0, help="seconds after start; 0 = no burst")
    p.add_argument("--burst-for", type=float, default=15)
    asyncio.run(main(p.parse_args()))
