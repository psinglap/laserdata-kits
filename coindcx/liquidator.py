"""A liquidation worker: holds every instrument's mark price and its shard of positions, and sends a liquidation
order for each position that the latest mark puts past its liquidation price.

Run several, each with --shard i --shards N. Each worker reads every partition of mark-prices (every worker needs
every instrument's mark, since a cross-margin account holds positions in many instruments) and its own share of
the positions partitions (accounts are sharded across workers), directly, on its own connections, with its own
offsets and nothing committed. On start it rebuilds marks and positions from the log before it acts.

Isolated margin: a long is liquidated when the mark is at or below its liquidation price, a short when at or
above. Each instrument keeps its longs and shorts in heaps ordered by liquidation price, so a new mark only touches
the positions it crosses. Cross margin: when an instrument's mark moves, every cross account holding it is
re-checked, in slices by a separate task; it is liquidated when maintenance margin / (wallet + unrealised PnL)
reaches 1 (CoinDCX's margin_ratio_cross rule).

Liquidation orders go to liquidation-orders from a separate task every 5 ms, so the read loops never wait on a
send. Prints the lane's latency every second (mark and position changes, from the feed handler or position book
to this worker: p50, p99, p99.9) with PASS or FAIL against --budget-ms, and when the whole mark batch was in.
"""
import argparse
import asyncio
import gc
import heapq
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, LIQ, MARKS, PARTITIONS, POSITIONS, STREAM, Window, connect, ensure_topics,
                    liq_price, load, maintenance_margin, msg, upnl)


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), MARKS, POSITIONS, LIQ)
    mark_r, pos_r, writer = await connect(a.url), await connect(a.url), await connect(a.url)
    start_ns = time.time_ns()

    mark = {}                                # pair -> (seq, mark price, ns)
    pos = {}                                 # position id -> latest state
    longs, shorts = defaultdict(list), defaultdict(list)   # pair -> heap of (key, position id, seq)
    cross_by_pair = defaultdict(set)         # pair -> cross accounts holding it
    acct_pos = defaultdict(set)              # cross account -> position ids
    wallet = {}
    sent = {}                                # position id -> seq already ordered (no duplicate orders)
    outbox = []
    batch_seen = defaultdict(int)
    lat, olat, st, run = Window(), Window(), defaultdict(float), defaultdict(int)
    caught = {"marks": False, "positions": False}
    complete = []                            # mark batches fully read, waiting for the cross check
    dirty = set()                            # cross accounts whose marks moved since the last check
    trigger = {"mark": False}                # was the check started by a new mark (timed) or a position change

    def ready():
        return caught["marks"] and caught["positions"]

    def order(pid, s, mp, mns, why):
        if sent.get(pid) == s["seq"]:
            return
        sent[pid] = s["seq"]
        now = time.time_ns()
        by_mark = trigger["mark"]
        outbox.append(SendMessage(msg(pid=pid, seq=s["seq"], pair=s["pair"], mp=mp, mns=mns, why=why, by=me, ns=now,
                                      bymark=by_mark)))
        if by_mark:
            olat.add((now - mns) / 1e6)      # timed from the mark that crossed it
        st["orders"] += 1

    def check_isolated(pair):
        m = mark.get(pair)
        if m is None:
            return
        _, mp, mns = m
        h = longs[pair]
        while h and -h[0][0] >= mp:          # longs: liquidation price at or above the mark
            _, pid, seq = heapq.heappop(h)
            s = pos.get(pid)
            if s and s["seq"] == seq:
                order(pid, s, mp, mns, "isolated")
        h = shorts[pair]
        while h and h[0][0] <= mp:           # shorts: liquidation price at or below the mark
            _, pid, seq = heapq.heappop(h)
            s = pos.get(pid)
            if s and s["seq"] == seq:
                order(pid, s, mp, mns, "isolated")

    def check_cross(acct):
        ids = acct_pos.get(acct)
        if not ids:
            return
        mm = pnl = 0.0
        newest = None
        for pid in ids:
            s = pos[pid]
            m = mark.get(s["pair"])
            if m is None:
                return
            mm += s["mm"]
            pnl += upnl(s["side"], s["entry"], s["qty"], m[1])
            if newest is None or m[2] > newest[2]:
                newest = m
        equity = wallet.get(acct, 0.0) + pnl
        if equity <= 0 or mm / equity >= 1:
            for pid in list(ids):
                order(pid, pos[pid], mark[pos[pid]["pair"]][1], newest[2], "cross")

    def drop(pid):
        s = pos.pop(pid, None)
        if s and s["mt"] == "crossed":
            acct_pos[s["acct"]].discard(pid)
            if not any(pos[q]["pair"] == s["pair"] for q in acct_pos[s["acct"]]):
                cross_by_pair[s["pair"]].discard(s["acct"])

    def apply_position(s):
        pid = s["pid"]
        old = pos.get(pid)
        if old and s["seq"] <= old["seq"]:
            run["repeats"] += 1
            return
        if s.get("closed"):
            drop(pid)
            sent.pop(pid, None)
            return
        if old and old["mt"] != s["mt"]:
            drop(pid)
        pos[pid] = s
        s["mm"] = maintenance_margin(s["entry"] * s["qty"])
        if s["mt"] == "isolated":
            lp = liq_price(s["side"], s["entry"], s["qty"], s["margin"])
            if s["side"] == "long":
                heapq.heappush(longs[s["pair"]], (-lp, pid, s["seq"]))
            else:
                heapq.heappush(shorts[s["pair"]], (lp, pid, s["seq"]))
            if ready():
                check_isolated(s["pair"])
        else:
            acct_pos[s["acct"]].add(pid)
            cross_by_pair[s["pair"]].add(s["acct"])
            wallet[s["acct"]] = s["wallet"]
            if ready():
                check_cross(s["acct"])

    def apply_mark(m):
        k = m["k"]
        if m["seq"] <= mark.get(k, (0,))[0]:
            run["repeats"] += 1
            return
        mark[k] = (m["seq"], m["mp"], m["ns"])
        if not ready():
            return
        trigger["mark"] = True
        check_isolated(k)
        trigger["mark"] = False
        dirty.update(cross_by_pair.get(k, ()))   # cross accounts holding it: re-checked once per read pass
        batch_seen[m["b"]] += 1
        if batch_seen[m["b"]] == m["n"]:
            complete.append(m["ns"])         # every instrument's mark is in; timed once cross accounts are checked
            del batch_seen[m["b"]]

    async def read(client, topic, parts, apply, key):
        who = {p: Consumer.Single(f"{me}-{topic}-{p}") for p in parts}
        nxt, done = {}, set()
        last = time.monotonic()
        while True:
            now_m = time.monotonic()
            st[f"gap_{key}"] = max(st[f"gap_{key}"], 1000 * (now_m - last))
            last, more = now_m, False
            for p in parts:
                ms = await client.poll_messages(STREAM, topic, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                                else PollingStrategy.First())
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        d = load(m.payload())
                        live = ready() and d["seq"] > 1 and d["ns"] >= start_ns
                        if live:
                            lat.add((now - d["ns"]) / 1e6)      # a live change, not the rebuild
                            st[key] += 1
                        else:
                            run["replayed"] += 1
                        apply(d)
                if len(ms) == a.batch:
                    more = True              # a rebuild or a burst: read on at once
                elif p not in done:
                    done.add(p)              # this partition has been read to the end of the log once
                    if len(done) == len(parts) and not caught[key]:
                        caught[key] = True
                        if ready():
                            on_caught_up()
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def cross_checker():
        # Cross accounts whose marks moved, checked once the whole mark batch is in (or 50 ms after the first
        # change, if marks arrive one by one), in slices so the read loops keep polling in between. Checking after
        # every partition instead would re-check an account holding several instruments many times per batch.
        since = None
        while True:
            await asyncio.sleep(0.001)
            if not ready() or not (dirty or complete):
                continue
            since = since or time.monotonic()
            if not complete and time.monotonic() - since < 0.05:
                continue
            since = None
            todo, done_b = list(dirty), list(complete)
            dirty.clear()
            complete.clear()
            for i in range(0, len(todo), 250):
                trigger["mark"] = True
                for acct in todo[i:i + 250]:
                    check_cross(acct)
                trigger["mark"] = False
                await asyncio.sleep(0)
            now = time.time_ns()
            for b_ns in done_b:
                st["batch_ms"] = max(st["batch_ms"], (now - b_ns) / 1e6)   # the whole batch checked, cross too

    def on_caught_up():
        gc.freeze()                          # keep the rebuilt state out of the collector's way
        for pair in list(mark):
            check_isolated(pair)
        for acct in list(acct_pos):
            check_cross(acct)
        print(f"[{a.name}] rebuilt from the log in {(time.time_ns() - start_ns) / 1e9:.2f}s: {len(mark)} marks, "
              f"{len(pos):,} positions ({len(acct_pos):,} cross accounts), {run['replayed']:,} records, "
              f"{int(st['orders']):,} orders on the first check", flush=True)

    async def send_orders():
        while True:
            await asyncio.sleep(0.005)
            if outbox:
                out, outbox[:] = list(outbox), []
                await writer.send_messages(STREAM, LIQ, Partitioning.balanced(), out)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not ready():
                print(f"[{a.name}] rebuilding from the log: {run['replayed']:,} records so far", flush=True)
                lat.flush()
                olat.flush()
                st.clear()
                continue
            r, o = lat.flush(), olat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {int(st['marks']):>5,} marks/s  {int(st['positions']):>5,} position changes/s  "
                  f"lane p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  "
                  f"{ok} (<{a.budget_ms:g}ms)  mark batch checked in {st['batch_ms']:.1f}ms  "
                  f"liquidation orders {int(st['orders']):,} (mark-to-order p99 {o['p99']:.1f}ms)  "
                  f"positions {len(pos):,}  read pass max {max(st['gap_marks'], st['gap_positions']):.0f}ms",
                  flush=True)
            st.clear()

    shard = [p for p in range(PARTITIONS[POSITIONS]) if p % a.shards == a.shard]
    await asyncio.gather(read(mark_r, MARKS, list(range(PARTITIONS[MARKS])), apply_mark, "marks"),
                         read(pos_r, POSITIONS, shard, apply_position, "positions"), cross_checker(), send_orders(),
                         report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="liquidator-1")
    p.add_argument("--shard", type=int, default=0)
    p.add_argument("--shards", type=int, default=1, help="workers sharing the accounts")
    p.add_argument("--budget-ms", type=float, default=50, help="lane p99 budget, feed or book to this worker")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
