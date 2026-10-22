"""DEXT's order updates: every status change of every client's order, written once to the live order update lane.

Each update is keyed by client ID, with a per-client sequence number and the order's resulting state. The status
values follow DhanHQ's Live Order Update docs:
  * TRANSIT: the order passed the OMS and RMS checks and is on its way to the exchange;
  * PENDING: the exchange has it (also after a modify, or a partial fill: traded quantity above zero);
  * TRADED: fully filled; CANCELLED: cancelled by the client, or the other exit leg of a Super Order;
  * REJECTED: failed a check.
A Super Order is an entry leg (LegNo 1) with a stop-loss leg (2) and a target leg (3): when the entry is TRADED,
both exit legs go PENDING; when one exit leg is TRADED, the other is CANCELLED. A modify on a stop-loss leg is a
trailing stop-loss moving.

Writers are on their own connections, each owning some partitions, and write every --tick-ms; an update created
at some point in the last tick waits for that write, and the wait counts in the latency the gateways measure.
--burst multiplies the rate for --burst-for seconds (the open: Dhan's AWS case study expects concurrent customers
to jump a hundredfold at market open).
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, INSTRUMENTS, PARTITIONS, STREAM, UPDATES, Window, client, connect, ensure_topics, msg, \
    order_no, part_of

WRITERS = PARTITIONS[UPDATES]          # one writer connection per partition


async def main(a):
    admin = await connect(a.url)
    await ensure_topics(admin, UPDATES)
    P = PARTITIONS[UPDATES]
    writers = [await connect(a.url) for _ in range(WRITERS)]   # partition p is written by writers[p % WRITERS]
    keys = [client(i) for i in range(a.clients)]
    rng = random.Random(11)
    seq = defaultdict(int)
    book = defaultdict(dict)             # client -> order no -> order dict (open orders only)
    n_orders = [0]
    acks, st = Window(), defaultdict(int)

    def update(k, oid, o, status, waited_ns):
        seq[k] += 1
        st[status] += 1
        seg, sym, _, _ = INSTRUMENTS[o["i"]]
        return k, SendMessage(msg(k=k, seq=seq[k], order_no=oid, leg=o["leg"], entry=o["entry"], segment=seg,
                                  symbol=sym, txn=o["txn"], product=o["product"], order_type=o["type"],
                                  status=status, qty=o["qty"], traded_qty=o["traded"], price=o["price"],
                                  avg_price=round(o["avg"], 2), ns=time.time_ns() - waited_ns))

    def new_no():
        n_orders[0] += 1
        return order_no(n_orders[0])

    def close_family(k, oid, waited_ns):
        # Cancel an order; for a Super Order exit leg, cancel the other exit leg with it.
        mine, out = book[k], []
        o = mine.pop(oid)
        out.append(update(k, oid, o, "CANCELLED", waited_ns))
        if o["leg"] > 1:
            for sib in [x for x, s in mine.items() if s["entry"] == o["entry"] and s["leg"] > 1]:
                out.append(update(k, sib, mine.pop(sib), "CANCELLED", waited_ns))
        return out

    def one_event(waited_ns):
        # A few clients (option scalpers, API algos) place far more orders than the long tail.
        k = keys[int(len(keys) * rng.random() ** 2)]
        mine = book[k]
        r = rng.random()
        if not mine or r < 0.38:
            out = []
            if len(mine) >= 8:               # the oldest goes first: open orders per client stay bounded
                st["evicted"] += 1
                out += close_family(k, next(iter(mine)), waited_ns)
            i = int(len(INSTRUMENTS) * rng.random() ** 1.5)
            _, _, lot, ref = INSTRUMENTS[i]
            px = round(ref * rng.uniform(0.98, 1.02), 2)
            oid = new_no()
            sup = rng.random() < 0.2
            o = {"i": i, "txn": rng.choice("BS") if not sup else "B", "qty": lot * rng.randint(1, 10), "traded": 0,
                 "price": px, "avg": 0.0, "leg": 1, "entry": oid if sup else "", "type": rng.choice(("LMT", "MKT")),
                 "product": "I" if sup or rng.random() < 0.6 else "C"}
            if rng.random() < 0.03:          # failed an RMS check (margin, price band): never reaches the exchange
                return out + [update(k, oid, o, "REJECTED", waited_ns)]
            mine[oid] = o
            st["super"] += sup
            return out + [update(k, oid, o, "TRANSIT", waited_ns), update(k, oid, o, "PENDING", waited_ns)]
        oid = rng.choice(list(mine))
        o = mine[oid]
        lot = INSTRUMENTS[o["i"]][2]
        if r < 0.75:                         # a fill: part of the order, or the rest of it
            left = o["qty"] - o["traded"]
            part = min(left, lot * max(1, int(o["qty"] / lot * rng.choice((0.25, 0.5, 1.0)))))
            fill_px = o["price"] * rng.uniform(0.999, 1.001)
            o["avg"] = (o["avg"] * o["traded"] + fill_px * part) / (o["traded"] + part)
            o["traded"] += part
            if o["traded"] < o["qty"]:
                return [update(k, oid, o, "PENDING", waited_ns)]
            del mine[oid]
            out = [update(k, oid, o, "TRADED", waited_ns)]
            if o["leg"] == 1 and o["entry"]:   # Super Order entry filled: stop-loss and target legs go live
                for leg, f in ((2, 0.97), (3, 1.04)):
                    lid = new_no()
                    mine[lid] = dict(o, leg=leg, txn="S", traded=0, avg=0.0, price=round(o["avg"] * f, 2),
                                     type="SL" if leg == 2 else "LMT")
                    out.append(update(k, lid, mine[lid], "PENDING", waited_ns))
                    st["legs"] += 1
            elif o["leg"] > 1:                 # one exit leg filled: the other is cancelled
                for sib in [x for x, s in mine.items() if s["entry"] == o["entry"] and s["leg"] > 1]:
                    out.append(update(k, sib, mine.pop(sib), "CANCELLED", waited_ns))
            return out
        if r < 0.87:                         # a modify; on a stop-loss leg, the trailing stop-loss moving up
            o["price"] = round(o["price"] * (rng.uniform(1.0, 1.01) if o["leg"] == 2 else rng.uniform(0.995, 1.005)), 2)
            st["trail" if o["leg"] == 2 else "modify"] += 1
            return [update(k, oid, o, "PENDING", waited_ns)]
        return close_family(k, oid, waited_ns)

    async def send(items):
        out = defaultdict(list)              # one batch per partition: a client's updates stay in order
        for k, m in items:
            out[part_of(k, P)].append(m)
        s0 = time.perf_counter()
        await asyncio.gather(*(writers[p % WRITERS].send_messages(STREAM, UPDATES, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))
        acks.add((time.perf_counter() - s0) * 1e3)

    async def run():
        gc.freeze()                          # keep start-up tables out of the collector's way
        t0 = due = time.monotonic()
        owed = 0.0
        while True:
            el = time.monotonic() - t0
            x = a.burst if a.burst_at and a.burst_at <= el < a.burst_at + a.burst_for else 1
            owed += a.rate * x * a.tick_ms / 1000
            items = []
            while owed >= 1:                 # an event can yield several updates (TRANSIT + PENDING, legs)
                ev = one_event(int(rng.random() * a.tick_ms * 1e6))
                items += ev
                owed -= len(ev)
            st["late_ms"] = max(st["late_ms"], 1000 * (time.monotonic() - due))
            if items:
                await send(items)
                st["updates"] += len(items)
            st["burst"] = int(x > 1)
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def show():
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[dext] {st['updates']:>6,} updates/s{' (open burst %gx)' % a.burst if st['burst'] else ''}  "
                  f"TRANSIT {st['TRANSIT']:,}  PENDING {st['PENDING']:,}  TRADED {st['TRADED']:,}  "
                  f"CANCELLED {st['CANCELLED']:,}  REJECTED {st['REJECTED']:,}  Super Orders {st['super']:,} "
                  f"(exit legs live {st['legs']:,}, trailing SL moves {st['trail']:,})  "
                  f"write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms  tick late max {st['late_ms']:.0f}ms",
                  flush=True)
            st.clear()

    print(f"[dext] {a.clients:,} clients with orders today, {len(INSTRUMENTS)} instruments, {a.rate:,.0f} updates/s, "
          f"{P} partitions keyed by client ID", flush=True)
    await asyncio.gather(run(), show())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--clients", type=int, default=300_000, help="clients with orders today")
    p.add_argument("--rate", type=float, default=10_000, help="order updates per second (all clients)")
    p.add_argument("--tick-ms", type=float, default=2, help="the writers write once per this many ms")
    p.add_argument("--burst", type=float, default=1, help="rate multiplier at the open")
    p.add_argument("--burst-at", type=float, default=0, help="seconds after start; 0 = no burst")
    p.add_argument("--burst-for", type=float, default=15)
    asyncio.run(main(p.parse_args()))
