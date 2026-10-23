"""The OMS's trade events: every order state change for every Broker API account, written once to its partner's log.

Alpaca's OMS v2 keeps account state in memory on sharded nodes and writes a write-ahead log to Redpanda; Alpaca's
OMS launch post says "other services can listen to trade and order related events by consuming the WAL". This
script stands in for the trade events that reach partners through the Broker API's trade events stream. Each event
is keyed by account ID, carries a per-account sequence number, a ULID event_id and the order's resulting state.
Event names follow Alpaca's trade_updates docs:
  * new: routed for execution; partial_fill / fill: some or all of the qty filled (fractional qty allowed);
  * canceled: a cancel was processed; replaced: a replace (new limit price) was processed;
  * trade_correct: a fill corrected after the fact (rare; Broker API trade events v2beta1).

Writers are on their own connections and write every --tick-ms, one batch per partition; an event created at some
point in the last tick waits for that write, and the wait counts in the latency the gateways measure.
--rebalance multiplies the robo-advisor partner's rate for --rebalance-for seconds: a scheduled rebalance across its
accounts (Alpaca's OMS launch post demonstrates 1 million fractional orders in under one minute).
"""
import argparse
import asyncio
import gc
import random
import time
import uuid
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, PARTNERS, PARTS, STREAM, SYMBOLS, Window, account, connect, ensure_topics, msg, \
    part_of, topic, ulid

WRITERS = 4


async def main(a):
    admin = await connect(a.url)
    await ensure_topics(admin)
    writers = [await connect(a.url) for _ in range(WRITERS)]
    rng = random.Random(7)
    accts = {code: [account(code, i) for i in range(max(1, int(a.accounts * share)))]
             for code, _, share, _ in PARTNERS}
    seq = defaultdict(int)
    book = defaultdict(dict)             # account -> order id -> open order
    acks, st = Window(), defaultdict(int)

    def event(code, acct, oid, o, ev, waited_ns, **extra):
        seq[acct] += 1
        st[ev] += 1
        now_ns = time.time_ns() - waited_ns
        return code, acct, SendMessage(msg(corr=code, k=acct, seq=seq[acct], event_id=ulid(now_ns // 1_000_000),
                                           event=ev, order_id=oid, symbol=o["sym"], side=o["side"],
                                           type=o["type"], qty=o["qty"], filled_qty=round(o["filled"], 6),
                                           filled_avg_price=round(o["avg"], 4), limit_price=o["limit"],
                                           status=o["status"], ns=now_ns, **extra))

    def one(code, waited_ns, rebalance=False):
        acct = accts[code][int(len(accts[code]) * rng.random() ** 2)]   # active traders place far more orders
        mine = book[acct]
        r = rng.random()
        if not mine or r < 0.36 or rebalance:
            out = []
            if len(mine) >= 6:               # bounded open orders per account: the oldest is canceled
                oid = next(iter(mine))
                o = mine.pop(oid)
                o["status"] = "canceled"
                out.append(event(code, acct, oid, o, "canceled", waited_ns))
            sym, ref = SYMBOLS[int(len(SYMBOLS) * rng.random() ** 1.4)]
            frac = rebalance or rng.random() < 0.5          # fractional notional orders are common on partner apps
            qty = round(rng.uniform(0.01, 3.0), 6) if frac else float(rng.randint(1, 50))
            if sym.endswith("/USD"):
                qty = round(rng.uniform(0.0005, 0.05), 6)
            mkt = rebalance or rng.random() < 0.6
            o = {"sym": sym, "ref": ref, "side": rng.choice(("buy", "sell")), "type": "market" if mkt else "limit",
                 "qty": qty, "filled": 0.0, "avg": 0.0, "limit": None if mkt else round(ref * rng.uniform(0.99, 1.01), 2),
                 "status": "new"}
            oid = str(uuid.UUID(int=rng.getrandbits(128)))
            mine[oid] = o
            st["rebalance"] += rebalance
            return out + [event(code, acct, oid, o, "new", waited_ns)]
        oid = rng.choice(list(mine))
        o = mine[oid]
        if r < 0.80:                         # a fill: part of the order or the rest of it
            left = o["qty"] - o["filled"]
            part = left if o["type"] == "market" or rng.random() < 0.5 else round(left * rng.uniform(0.2, 0.7), 6)
            px = o["ref"] * rng.uniform(0.999, 1.001)
            o["avg"] = (o["avg"] * o["filled"] + px * part) / (o["filled"] + part)
            o["filled"] += part
            if o["qty"] - o["filled"] > 1e-9:
                o["status"] = "partially_filled"
                return [event(code, acct, oid, o, "partial_fill", waited_ns, price=round(px, 4), fill_qty=part)]
            o["status"] = "filled"
            del mine[oid]
            out = [event(code, acct, oid, o, "fill", waited_ns, price=round(px, 4), fill_qty=part)]
            if rng.random() < 0.002:         # a corrected fill price, after the fact
                o["avg"] *= rng.uniform(0.9995, 1.0005)
                out.append(event(code, acct, oid, o, "trade_correct", waited_ns))
            return out
        if r < 0.90 and o["type"] == "limit":
            o["limit"] = round(o["limit"] * rng.uniform(0.995, 1.005), 2)
            return [event(code, acct, oid, o, "replaced", waited_ns)]
        o["status"] = "canceled"
        del mine[oid]
        return [event(code, acct, oid, o, "canceled", waited_ns)]

    async def send(items):
        out = defaultdict(list)              # one batch per (partner, partition): an account's events stay in order
        for code, acct, m in items:
            out[(code, part_of(acct, PARTS[code]))].append(m)
        s0 = time.perf_counter()
        await asyncio.gather(*(writers[i % WRITERS].send_messages(STREAM, topic(code), Partitioning.partition_id(p), ms)
                               for i, ((code, p), ms) in enumerate(out.items())))
        acks.add((time.perf_counter() - s0) * 1e3)

    async def run():
        gc.freeze()                          # keep start-up tables out of the collector's way
        t0 = due = time.monotonic()
        owed = defaultdict(float)
        while True:
            el = time.monotonic() - t0
            reb = a.rebalance_at and a.rebalance_at <= el < a.rebalance_at + a.rebalance_for
            items = []
            for code, _, share, _ in PARTNERS:
                x = a.rebalance if reb and code == PARTNERS[0][0] else 1
                owed[code] += a.rate * share * x * a.tick_ms / 1000
                while owed[code] >= 1:
                    ev = one(code, int(rng.random() * a.tick_ms * 1e6), rebalance=x > 1 and rng.random() < 0.7)
                    items += ev
                    owed[code] -= len(ev)
            st["late_ms"] = max(st["late_ms"], 1000 * (time.monotonic() - due))
            if items:
                await send(items)
                st["events"] += len(items)
            st["reb"] = int(bool(reb))
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def show():
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[oms] {st['events']:>6,} trade events/s{' (rebalance %gx on %s)' % (a.rebalance, PARTNERS[0][0]) if st['reb'] else ''}  "
                  f"new {st['new']:,}  partial_fill {st['partial_fill']:,}  fill {st['fill']:,}  "
                  f"canceled {st['canceled']:,}  replaced {st['replaced']:,}  trade_correct {st['trade_correct']:,}  "
                  f"rebalance orders {st['rebalance']:,}  write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms  "
                  f"tick late max {st['late_ms']:.0f}ms", flush=True)
            st.clear()

    print(f"[oms] {sum(map(len, accts.values())):,} accounts across {len(PARTNERS)} partners, {len(SYMBOLS)} symbols, "
          f"{a.rate:,.0f} trade events/s, one log per partner keyed by account ID", flush=True)
    await asyncio.gather(run(), show())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--accounts", type=int, default=1_000_000, help="Broker API accounts with orders today")
    p.add_argument("--rate", type=float, default=10_000, help="trade events per second, all partners")
    p.add_argument("--tick-ms", type=float, default=2, help="the writers write once per this many ms")
    p.add_argument("--rebalance", type=float, default=1, help="rate multiplier for the robo-advisor's rebalance")
    p.add_argument("--rebalance-at", type=float, default=0, help="seconds after start; 0 = no rebalance")
    p.add_argument("--rebalance-for", type=float, default=15)
    asyncio.run(main(p.parse_args()))
