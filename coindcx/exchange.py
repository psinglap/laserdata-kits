"""The source of truth: the futures mark price feed handler, the position book and the order path, in one process.

Mark prices: the partner exchange's last price for every instrument moves all the time; once a second, on the
whole second, a mark price for every instrument arrives at once (in CoinDCX's public futures prices every mark
carries a partner send time, bmST, on the whole second). The feed handler writes the whole batch to the
mark-prices topic, one record per instrument, keyed by instrument, with a per-instrument sequence number and the
resulting mark price (never a delta). A crash (--crash-at, --crash-pct, --crash-for) drags every instrument down
together, harder for some than others.

Positions: accounts open, add to, reduce and close USDT-margined positions (isolated or cross margin, 2x to 25x),
written to the positions topic keyed by account with the resulting state.

Order path: the liquidation workers (liquidator.py) send liquidation orders to liquidation-orders. Each one is
filled at the last price at the moment the worker sent it, and the position closes. For comparison it also fills
the same order at the last price --lag seconds later, as a worker running --lag seconds behind the lane would
have. Every second it prints how far past the liquidation price those fills landed (margin the position no longer
had) and how much fell past the bankruptcy price (what the insurance fund pays), live and --lag behind.
"""
import argparse
import asyncio
import bisect
import gc
import heapq
import itertools
import math
import random
import time
from collections import defaultdict, deque

from apache_iggy import Partitioning, SendMessage

from common import (DEFAULT_URL, LIQ, MARKS, PARTITIONS, POSITIONS, STREAM, Window, account, bankruptcy_price,
                    connect, ensure_topics, instruments, liq_price, load, msg, part_of, read_partitions, start_price)

LEVERAGE = (2, 3, 5, 10, 15, 20, 25)
LEV_WEIGHTS = (8, 10, 22, 30, 12, 10, 8)


async def main(a):
    admin = await connect(a.url)
    await ensure_topics(admin, MARKS, POSITIONS, LIQ)
    mark_w, pos_w = await connect(a.url), await connect(a.url)          # one connection per writer and reader
    orders = await connect(a.url)

    rng = random.Random(11)
    pairs = instruments(a.instruments)
    weight = [8.0 if i < 20 else 1.0 for i in range(len(pairs))]       # majors carry most open interest
    beta = {p: (1.0 if i < 2 else rng.uniform(0.7, 1.6)) for i, p in enumerate(pairs)}
    ltp = {p: start_price(p) for p in pairs}
    hist = {p: deque(maxlen=int(3 / 0.01) + 50) for p in pairs}       # (ns, last price) every 10 ms, ~3 s
    mark = dict(ltp)
    mseq = defaultdict(int)
    pos = {}                                  # position id -> state
    by_acct = defaultdict(set)
    cross = {}                                # cross-margin account -> wallet balance in USDT
    pseq = defaultdict(int)
    n_acct = max(1, a.positions // 2)
    lagged = []                               # heap of (due ns, n, pair, side, qty, liq, bkr)
    tie = itertools.count()
    st, lat = defaultdict(float), Window()

    def walk(dt, crashing):
        t = time.time_ns()
        for p in pairs:
            drift = -beta[p] * a.crash_pct / 100 / a.crash_for * dt if crashing else 0.0
            vol = a.vol * (2.5 if crashing else 1.0) * beta[p] * math.sqrt(dt)
            ltp[p] *= math.exp(drift + rng.gauss(0, vol))
            hist[p].append((t, ltp[p]))

    def price_at(p, t):
        h = hist[p]
        i = bisect.bisect_right(h, (t, math.inf)) - 1
        return h[max(0, i)][1] if h else ltp[p]

    def state(pid):
        s = pos[pid]
        pseq[pid] += 1
        s["seq"] = pseq[pid]
        d = dict(s, pid=pid)
        if s["acct"] in cross:
            d["wallet"] = cross[s["acct"]]
        return s["acct"], SendMessage(msg(**d))

    def closed(pid, acct, why):
        pseq[pid] += 1
        return acct, SendMessage(msg(pid=pid, acct=acct, seq=pseq[pid], closed=why))

    def open_position(acct=None):
        acct = acct or account(rng.randrange(n_acct))
        p = rng.choices(pairs, weight)[0]
        pid = f"{acct}:{p}"
        if pid in pos:
            return None
        if acct not in cross and not by_acct[acct] and rng.random() < a.cross_share:
            cross[acct] = 0.0
        lev = rng.choices(LEVERAGE, LEV_WEIGHTS)[0]
        notional = min(400_000.0, rng.lognormvariate(math.log(600), 1.2))
        side = "long" if rng.random() < a.long_share else "short"
        entry = ltp[p]
        qty = notional / entry
        margin = notional / lev
        pos[pid] = dict(acct=acct, pair=p, side=side, qty=qty, entry=entry, lev=lev, margin=margin,
                        mt="crossed" if acct in cross else "isolated")
        by_acct[acct].add(pid)
        if acct in cross:
            cross[acct] += margin * rng.uniform(1.05, 1.8)             # wallet backing every cross position
        return state(pid)

    def one_change():
        r = rng.random()
        if r < 0.45 or len(pos) < a.positions * 0.9:
            return open_position()
        pid = rng.choice(list_ids[0]) if list_ids[0] else None
        if pid not in pos:
            return open_position()
        s = pos[pid]
        if r < 0.70:                                                    # add at the current price
            add = s["qty"] * rng.uniform(0.1, 0.5)
            s["entry"] = (s["entry"] * s["qty"] + ltp[s["pair"]] * add) / (s["qty"] + add)
            s["qty"] += add
            s["margin"] += add * ltp[s["pair"]] / s["lev"]
            return state(pid)
        if r < 0.85:                                                    # reduce
            f = rng.uniform(0.2, 0.7)
            s["qty"] *= 1 - f
            s["margin"] *= 1 - f
            return state(pid)
        del pos[pid]                                                    # close
        by_acct[s["acct"]].discard(pid)
        return closed(pid, s["acct"], "closed")

    list_ids = [[]]
    closes = []                              # positions closed by liquidation, for the next tick

    def batches(topic, items):
        out = defaultdict(list)
        for k, m in items:
            out[part_of(k, PARTITIONS[topic])].append(m)
        return out.items()

    async def send(writer, topic, items):
        await asyncio.gather(*(writer.send_messages(STREAM, topic, Partitioning.partition_id(p), ms)
                               for p, ms in batches(topic, items)))

    async def send_marks(b):
        # The whole batch carries one timestamp, taken before it is encoded: latency counts from the moment the
        # batch arrived at the feed handler, so encoding and queueing the sends are inside the measurement.
        ns, n, items = time.time_ns(), len(pairs), []
        for p in pairs:
            mark[p] = 0.5 * mark[p] + 0.5 * ltp[p]                     # the mark trails the last price a little
            mseq[p] += 1
            body = f'{{"k":"{p}","seq":{mseq[p]},"mp":{mark[p]:.10g},"b":{b},"n":{n},"ns":{ns}}}'
            items.append((p, SendMessage(body.encode())))
        await send(mark_w, MARKS, items)

    async def snapshot():
        # The starting book (seq 1 per position, not timed as live changes) and one mark batch for every instrument.
        ch = [x for x in (open_position() for _ in range(int(a.positions * 1.05))) if x]
        for i in range(0, len(ch), 5000):
            await send(pos_w, POSITIONS, ch[i:i + 5000])
        await send_marks(0)
        print(f"[exchange] snapshot: {len(pos):,} open positions on {len(pairs)} instruments, "
              f"{len(cross):,} cross-margin accounts", flush=True)

    async def run():
        await snapshot()
        gc.freeze()                          # keep start-up tables out of the collector's way
        t0 = due = time.monotonic()
        next_mark, b, owed = t0 + 1.0, 0, 0.0
        while True:
            t = time.monotonic() - t0
            crashing = a.crash_at and a.crash_at <= t < a.crash_at + a.crash_for
            walk(0.01, crashing)
            owed += a.rate / 100
            if int(owed) and (not list_ids[0] or rng.random() < 0.05):
                list_ids[0] = list(pos)
            ch = [x for x in (one_change() for _ in range(int(owed))) if x]
            owed -= int(owed)
            if closes:
                ch += closes
                closes.clear()
            t_send = time.monotonic()
            jobs = [send(pos_w, POSITIONS, ch)] if ch else []
            if time.monotonic() >= next_mark:
                b += 1
                next_mark += 1.0                 # from the due time, so a slow tick does not drift the batches
                jobs.append(send_marks(b))
                st["batches"] += 1
            if jobs:
                await asyncio.gather(*jobs)
            st["send_ms"] = max(st["send_ms"], 1000 * (time.monotonic() - t_send))
            st["late_ms"] = max(st["late_ms"], 1000 * (t_send - due))
            st["changes"] += len(ch)
            st["crash"] = int(bool(crashing))
            settle_lagged()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    def past(side, qty, liq, bkr, fill):
        # USDT past the liquidation price (margin the position no longer had) and past bankruptcy (insurance fund).
        if side == "long":
            return max(0.0, (liq - fill) * qty), max(0.0, (bkr - fill) * qty)
        return max(0.0, (fill - liq) * qty), max(0.0, (fill - bkr) * qty)

    def settle_lagged():
        now = time.time_ns()
        while lagged and lagged[0][0] <= now:
            due_ns, _, p, side, qty, liq, bkr = heapq.heappop(lagged)
            over, fund = past(side, qty, liq, bkr, price_at(p, due_ns))
            st["lag_over"] += over
            st["lag_fund"] += fund
            st["lag_n"] += 1

    async def on_order(m):
        o = load(m.payload())
        s = pos.get(o["pid"])
        if s is None or pseq[o["pid"]] != o["seq"]:
            st["stale"] += 1                 # the position changed or closed after the worker saw it
            return
        p, side, qty = s["pair"], s["side"], s["qty"]
        if s["mt"] == "isolated":
            liq = liq_price(side, s["entry"], qty, s["margin"])
            bkr = bankruptcy_price(side, s["entry"], qty, s["margin"])
        else:                                # cross: measured against the mark the worker acted on
            liq, bkr = o["mp"], bankruptcy_price(side, s["entry"], qty, s["margin"])
        over, fund = past(side, qty, liq, bkr, price_at(p, o["ns"]))
        st["over"] += over
        st["fund"] += fund
        st["filled"] += 1
        if o["bymark"]:
            lat.add((time.time_ns() - o["mns"]) / 1e6)     # from the mark that crossed it to the fill
        heapq.heappush(lagged, (o["ns"] + int(a.lag * 1e9), next(tie), p, side, qty, liq, bkr))
        del pos[o["pid"]]
        by_acct[s["acct"]].discard(o["pid"])
        closes.append(closed(o["pid"], s["acct"], "liquidated"))     # sent with the next tick, not from here

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            print(f"[exchange] {int(st['changes']):>5,} position changes/s  mark batches {int(st['batches'])} "
                  f"({'crash' if st['crash'] else 'steady'})  liquidations filled {int(st['filled']):,}  "
                  f"stale {int(st['stale']):,}  mark-to-fill p99 {r['p99']:.1f}ms  "
                  f"past liquidation price {st['over']:,.0f} USDT  past bankruptcy {st['fund']:,.0f} USDT  "
                  f"a worker {a.lag:g}s behind: past liquidation {st['lag_over']:,.0f} USDT  "
                  f"past bankruptcy {st['lag_fund']:,.0f} USDT ({int(st['lag_n']):,} fills)  "
                  f"open {len(pos):,}  tick late max {st['late_ms']:.0f}ms  send max {st['send_ms']:.0f}ms",
                  flush=True)
            st.clear()

    await asyncio.gather(run(), read_partitions(orders, LIQ, range(PARTITIONS[LIQ]), "order-path", on_order,
                                                         commit_s=1, batch=1000, poll_ms=0.5), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--instruments", type=int, default=505, help="active futures instruments")
    p.add_argument("--positions", type=int, default=300_000, help="open positions")
    p.add_argument("--rate", type=float, default=2_000, help="position changes per second (fills, adds, closes)")
    p.add_argument("--cross-share", type=float, default=0.25, help="share of accounts on cross margin")
    p.add_argument("--long-share", type=float, default=0.65, help="share of positions that are long")
    p.add_argument("--vol", type=float, default=0.0015, help="price volatility per sqrt(second)")
    p.add_argument("--crash-at", type=float, default=0, help="seconds after start (0 = no crash)")
    p.add_argument("--crash-for", type=float, default=15)
    p.add_argument("--crash-pct", type=float, default=6, help="how far the market falls over the crash, in percent")
    p.add_argument("--lag", type=float, default=0.5, help="seconds behind, for the comparison fills")
    asyncio.run(main(p.parse_args()))
