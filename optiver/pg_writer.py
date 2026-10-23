"""Stands in for Postgres plus PG feed's decoding path: committed changes to config and pricing tables,
each written once to the change log.

Clients commit small transactions to small, busy tables (a few rows of one table each). Every committed
row change is written to its table's partition with the resulting row, the table's sequence number, the
commit LSN, the commit time and the table's state hash after the change. One connection per partition,
writing once every --tick-ms (a change waits for the next write, and that wait counts in the latency).

On start it writes every table's current rows once (op "S"), the state a first subscriber would load.
In your environment this script is replaced by pg_bridge.py, which reads the same changes from a
logical decoding slot.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import CHANGES, STREAM, DEFAULT_URL, Window, connect, encode, ensure_topics, partition_of, row_hash, tables


def make_row(rng, base: str) -> str:
    # Small rows, as in a config or pricing table: a few numeric fields and a version.
    if base.startswith("pricing"):
        return f"{rng.uniform(0.05, 0.9):.5f},{rng.uniform(-0.2, 0.2):.5f},{rng.uniform(0.0, 0.05):.5f},{rng.randint(1, 9999)}"
    return f"{rng.randint(1, 50)},{rng.choice(('on', 'off', 'wide', 'tight'))},{rng.randint(1, 100_000)},{rng.randint(1, 9999)}"


async def main(a):
    admin = await connect(a.url)
    await ensure_topics(admin, a.partitions)
    names = tables(a.tables)
    part = {t: partition_of(t, a.partitions) for t in names}
    rng = random.Random(11)
    rows = {t: {f"r{i:05d}": make_row(rng, t) for i in range(a.rows)} for t in names}
    h = {t: sum(row_hash(k, r) for k, r in rows[t].items()) & 0xFFFFFFFF for t in names}
    seq = dict.fromkeys(names, 0)
    next_key = dict.fromkeys(names, a.rows)
    lsn = [0x16B3_0000_0000]
    conns = [await connect(a.url) for _ in range(a.partitions)]

    # Startup state: every row once, op "S", sequence 1..rows. Not a live change.
    hh = dict.fromkeys(names, 0)
    for t in names:
        batch = []
        for k, r in rows[t].items():
            seq[t] += 1
            hh[t] = (hh[t] + row_hash(k, r)) & 0xFFFFFFFF
            batch.append(SendMessage(encode(t, k, "S", r, seq[t], lsn[0], time.time_ns(), hh[t])))
        for i in range(0, len(batch), 2000):
            await conns[part[t]].send_messages(STREAM, CHANGES, part[t], batch[i:i + 2000])
    print(f"[writer] startup state: {a.tables} tables x {a.rows:,} rows on {a.partitions} partitions", flush=True)
    gc.freeze()                                   # keep the startup tables out of the collector's way

    # A few tables are hot (the pricing inputs that move all session); most change now and then.
    weights = [1.0 / (1 + i) ** 1.2 for i in range(len(names))]
    acks, st, t0 = Window(), dict(changes=0, commits=0), time.monotonic()

    def commit(waited_ns):
        """One transaction: 1..--txn-rows changes to one table, one commit LSN. Returns (partition, messages)."""
        t = rng.choices(names, weights)[0]
        lsn[0] += rng.randint(40, 400)
        out = []
        for _ in range(rng.randint(1, a.txn_rows)):
            r = rng.random()
            if r < a.churn / 2 and len(rows[t]) > 1:             # a row removed
                k = rng.choice(list(rows[t]))
                h[t] = (h[t] - row_hash(k, rows[t].pop(k))) & 0xFFFFFFFF
                op, new = "D", ""
            elif r < a.churn:                                     # a row added
                k = f"r{next_key[t]:05d}"
                next_key[t] += 1
                new = make_row(rng, t)
                rows[t][k] = new
                h[t] = (h[t] + row_hash(k, new)) & 0xFFFFFFFF
                op = "I"
            else:                                                 # a row updated in place
                k = f"r{int(len(rows[t]) * rng.random() ** 2):05d}"
                if k not in rows[t]:
                    k = next(iter(rows[t]))
                new = make_row(rng, t)
                h[t] = (h[t] - row_hash(k, rows[t][k]) + row_hash(k, new)) & 0xFFFFFFFF
                rows[t][k] = new
                op = "U"
            seq[t] += 1
            out.append(SendMessage(encode(t, k, op, new, seq[t], lsn[0], time.time_ns() - waited_ns, h[t])))
        st["commits"] += 1
        return part[t], out

    async def send(pid, batch):
        s0 = time.perf_counter()
        await conns[pid].send_messages(STREAM, CHANGES, pid, batch)
        acks.add((time.perf_counter() - s0) * 1e3)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            el = time.monotonic() - t0
            burst = f"  burst {a.burst:g}x" if a.burst_at and a.burst_at <= el < a.burst_at + a.burst_for else ""
            print(f"[writer] {st['changes']:>6,} row changes/s in {st['commits']:,} commits  {r['n']:,} writes  "
                  f"write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms  LSN {lsn[0] >> 32:X}/{lsn[0] & 0xFFFFFFFF:08X}{burst}",
                  flush=True)
            st.update(changes=0, commits=0)

    async def run():
        due, carry = time.monotonic(), 0.0
        while True:
            el = time.monotonic() - t0
            x = a.burst if a.burst_at and a.burst_at <= el < a.burst_at + a.burst_for else 1
            carry += a.rate * x * a.tick_ms / 1000
            per = {}
            while carry >= 1:
                # A commit lands at some point in the last --tick-ms and waits for this write; that wait counts.
                pid, ms = commit(int(rng.random() * a.tick_ms * 1e6))
                per.setdefault(pid, []).extend(ms)
                carry -= len(ms)
                st["changes"] += len(ms)
            if per:
                await asyncio.gather(*(send(pid, b) for pid, b in per.items()))   # one batch per partition
            due += a.tick_ms / 1000                                              # from the due time, not from now
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    await asyncio.gather(report(), run())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--tables", type=int, default=32, help="config and pricing tables")
    p.add_argument("--rows", type=int, default=2000, help="rows per table at start")
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--rate", type=int, default=5000, help="row changes per second, all tables")
    p.add_argument("--txn-rows", type=int, default=4, help="most rows one transaction changes")
    p.add_argument("--churn", type=float, default=0.02, help="share of changes that insert or delete a row")
    p.add_argument("--tick-ms", type=float, default=1, help="write once per this many ms")
    p.add_argument("--burst", type=float, default=1, help="change-rate multiplier during the burst")
    p.add_argument("--burst-at", type=float, default=0, help="seconds after start; 0 = no burst")
    p.add_argument("--burst-for", type=float, default=10)
    asyncio.run(main(p.parse_args()))
