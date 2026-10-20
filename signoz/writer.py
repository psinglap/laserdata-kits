"""ClickHouse writer: drain the otlp_logs topic into ClickHouse, and commit only what ClickHouse has.

One consumer group ("clickhouse-writer") reads every partition. Rows collect in memory and go to
ClickHouse in one insert every --flush-ms (capped at --max-rows). Only after ClickHouse confirms the
insert does the writer store each partition's offset, so a crash, a restart or a stalled ClickHouse
loses nothing: the next writer resumes from the last confirmed batch. Rows that were inserted but not
yet committed come back once after a crash; the ReplacingMergeTree key (tenant, batch_seq, idx)
folds them away.

When ClickHouse stops accepting inserts, the writer stops reading once --max-pending rows are waiting
(backpressure on the log, not on agents) and the batches stay in Apache Iggy until it can write again.

Prints, every second: rows written, receive-to-queryable latency (collector accept to ClickHouse
insert confirmed) p50/p99/p99.9 with PASS or FAIL against --budget-ms, the time batches spent in the
log before the writer read them, and how far behind the writer is.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict
from datetime import timedelta

import clickhouse_connect
from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import CH, DEFAULT_URL, LOGS, STREAM, TABLE, Window, load

DDL = [f"CREATE DATABASE IF NOT EXISTS {TABLE.split('.')[0]}",
       f"""CREATE TABLE IF NOT EXISTS {TABLE} (
             tenant LowCardinality(String), service LowCardinality(String), timestamp UInt64,
             severity LowCardinality(String), body String, batch_seq UInt64, idx UInt32, received_ns UInt64)
           ENGINE = ReplacingMergeTree ORDER BY (tenant, batch_seq, idx)"""]
COLUMNS = ["tenant", "service", "timestamp", "severity", "body", "batch_seq", "idx", "received_ns"]


def to_rows(d: dict) -> list:
    """Map one message to table rows. Batches from collectors.py carry recs; a bridged OTLP batch does too."""
    return [[d["t"], d.get("svc", ""), ts, sev, body, d["seq"], i, d["ns"]] for i, (ts, sev, body) in enumerate(d["recs"])]


async def main(a):
    ch = clickhouse_connect.get_client(**CH, send_receive_timeout=120)
    for q in DDL:
        ch.command(q)

    async def own_client():                 # one connection per reader and writer, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await own_client()
    if await admin.get_topic(STREAM, LOGS) is None:          # collectors.py may not have created it yet
        try:
            await admin.create_stream(STREAM)
        except RuntimeError:
            pass
        try:
            await admin.create_topic(STREAM, LOGS, a.partitions)
        except RuntimeError:
            pass                                             # created by collectors.py in the meantime
    while await admin.get_topic(STREAM, LOGS) is None:
        print("[writer] waiting for the otlp_logs topic…", flush=True)
        await asyncio.sleep(1)

    group = await (await own_client()).consumer_group(
        a.group, STREAM, LOGS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
        auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(milliseconds=1))

    pending = []                            # (partition, offset, accepted_ns, rows) in arrival order
    pending_rows = [0]
    room = asyncio.Event()
    room.set()
    fresh, queue, stats = Window(), Window(), defaultdict(int)
    state = {"last_insert": time.monotonic(), "inserting_since": None, "mode": "catching up"}
    gc.freeze()

    async def on_batch(m):
        d = load(m.payload())
        rows = to_rows(d)
        queue.add((time.time_ns() - d["ns"]) / 1e6)
        pending.append((m.partition_id(), m.offset(), d["ns"], rows))
        pending_rows[0] += len(rows)
        if pending_rows[0] >= a.max_pending:
            room.clear()
            await room.wait()               # stop reading; the batches wait in the log, not in agents or memory

    async def flush():
        while True:
            await asyncio.sleep(a.flush_ms / 1000)
            if not pending:
                continue
            take, n = 0, 0
            while take < len(pending) and n < a.max_rows:
                n += len(pending[take][3])
                take += 1
            chunk = pending[:take]
            rows = [r for _, _, _, rs in chunk for r in rs]
            state["inserting_since"] = time.monotonic()
            while True:
                try:
                    await asyncio.to_thread(ch.insert, TABLE, rows, column_names=COLUMNS)
                    break
                except Exception as e:      # ClickHouse down or refusing: keep the rows, commit nothing, retry
                    stats["insert_errors"] += 1
                    print(f"[writer] insert failed, will retry: {str(e)[:120]}", flush=True)
                    await asyncio.sleep(1)
            done = time.time_ns()
            state["inserting_since"], state["last_insert"] = None, time.monotonic()
            last = {}
            for pid, off, _, _ in chunk:
                last[pid] = max(off, last.get(pid, -1))
            for pid, off in last.items():   # commit only what ClickHouse has confirmed
                await group.store_offset(off, pid)
            for _, _, ns, rs in chunk:
                fresh.add((done - ns) / 1e6)
            del pending[:take]
            pending_rows[0] -= len(rows)
            stats["rows"] += len(rows)
            stats["inserts"] += 1
            if pending_rows[0] < a.max_pending:
                room.set()

    async def report():
        while True:
            await asyncio.sleep(1)
            r, q = fresh.flush(), queue.flush()
            behind = (time.time_ns() - pending[0][2]) / 1e9 if pending else 0.0
            tail = (f"queue p99 {q['p99']:6.1f}ms  inserts {stats['inserts']}/s  "
                    f"waiting in writer {pending_rows[0]:,} rows  oldest unwritten {behind:5.1f}s")
            if state["inserting_since"] and time.monotonic() - state["inserting_since"] > 2:
                stalled = time.monotonic() - state["inserting_since"]
                state["mode"] = "draining"
                print(f"[writer] ClickHouse has not confirmed an insert for {stalled:4.1f}s; reading paused, "
                      f"batches wait in Apache Iggy  {tail}", flush=True)
            elif state["mode"] != "live":
                # live again only after a whole second of rows inside the budget: a backlog drains over several seconds
                if r["n"] and r["max"] < a.budget_ms:
                    state["mode"] = "live"
                print(f"[writer] {state['mode']}: wrote {stats['rows']:,} rows this second  {tail}", flush=True)
            else:
                ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
                print(f"[writer] {stats['rows']:>8,} rows/s  receive-to-queryable p50 {r['p50']:6.1f}ms  "
                      f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  {tail}", flush=True)
            stats.clear()

    await asyncio.gather(group.consume_messages(on_batch, asyncio.Event()), flush(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--group", default="clickhouse-writer")
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--flush-ms", type=int, default=250, help="one ClickHouse insert at most this often")
    p.add_argument("--max-rows", type=int, default=100_000, help="rows per insert, at most")
    p.add_argument("--max-pending", type=int, default=300_000, help="stop reading the log above this many rows in memory")
    p.add_argument("--budget-ms", type=int, default=1000, help="receive-to-queryable p99 budget")
    asyncio.run(main(p.parse_args()))
