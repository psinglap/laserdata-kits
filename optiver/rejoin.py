"""Processes that start or restart: each one rebuilds its tables from the log, with no snapshot query on Postgres.

Every --every-s seconds, --storm simulated processes start at once (a deploy, a host restart, the
morning start-up). Each subscribes to --tables-each tables and, for each of its tables' partitions:
  1. reads the latest checkpoint from `table-checkpoints` (one message, PollingStrategy.Last);
  2. replays the change log from the offset after that checkpoint up to the moment it started;
  3. checks every table against the writer's state hash and its sequence for gaps.
"Rejoin time" is from start to a full, checked copy of its tables. The rows it loads are rows a
Postgres snapshot would otherwise have read for it, on the database clients write to.

The changes a rejoining process replays were written before it started, so they are counted as
replayed, never as live latency. Live latency is subscriber.py's job.
"""
import argparse
import asyncio
import gc
import json
import random
import time

from apache_iggy import Consumer, PollingStrategy

from common import CHANGES, CHECKPOINTS, DEFAULT_URL, STREAM, Table, Window, connect, partition_of, row_hash, tables, wait_for_topics


async def main(a):
    admin = await connect(a.url)
    parts = await wait_for_topics(admin, "rejoin")
    names = a.names.split(",") if a.names else tables(a.tables)
    rng = random.Random(5)
    conns = [await connect(a.url) for _ in range(a.conns)]    # a pool: one rejoin in flight per connection
    me = Consumer.Single("rejoin")
    times, st = Window(), dict(done=0, rows=0, replayed=0, ok=0, bad=0, gaps=0, nocp=0, storms=0)
    total = dict(done=0, rows=0, ok=0, bad=0, gaps=0)
    gc.freeze()

    async def rejoin(c, mine):
        t0 = time.time_ns()
        copy, bad = {t: Table() for t in mine}, False
        for pid in sorted({partition_of(t, parts) for t in mine}):
            ms = await c.poll_messages(STREAM, CHECKPOINTS, consumer=me, partition_id=pid, count=1, auto_commit=False,
                                       polling_strategy=PollingStrategy.Last())
            if not ms:
                return "nocp"
            cp = json.loads(ms[0].payload())
            for t in mine:
                if t in cp["tables"]:
                    copy[t].load(cp["tables"][t])
                    st["rows"] += len(copy[t].rows)
            nxt = cp["off"] + 1
            while True:                              # replay from the checkpoint to the moment this process started
                ms = await c.poll_messages(STREAM, CHANGES, consumer=me, partition_id=pid, count=a.batch,
                                           auto_commit=False, polling_strategy=PollingStrategy.Offset(nxt))
                done = not ms or len(ms) < a.batch
                for m in ms:
                    ch = json.loads(m.payload())
                    if ch["n"] > t0:
                        done = True                  # written after this process started: the live feed's job
                        break
                    tab = copy.get(ch["t"])
                    if tab is None:
                        continue                     # a table this process does not subscribe to
                    res = tab.apply(ch)
                    st["replayed"] += res != "repeat"
                    st["gaps"] += res.startswith("gap")
                    bad |= res.endswith("bad")       # this copy's hash differs from the database's after this change
                if done:
                    break
                nxt = ms[-1].offset() + 1
        times.add((time.time_ns() - t0) / 1e6)
        # The copy must match the database: every change matched the writer's hash, and the rows add up to it.
        rows_ok = all(x.seq > 0 and sum(row_hash(k, r) for k, r in x.rows.items()) & 0xFFFFFFFF == x.h
                      for x in copy.values())
        return "ok" if rows_ok and not bad else "bad"

    async def worker(c, q):
        while True:
            mine = await q.get()
            res = await rejoin(c, mine)
            if res == "nocp":
                st["nocp"] += 1
            else:
                st["done"] += 1
                st[res] += 1
            q.task_done()

    q = asyncio.Queue()
    for c in conns:
        asyncio.create_task(worker(c, q))

    async def storms():
        due = time.monotonic() + a.start_s
        while True:
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            due += a.every_s
            st["storms"] += 1
            for _ in range(a.storm):
                q.put_nowait(rng.sample(names, min(a.tables_each, len(names))))

    async def report():
        while True:
            await asyncio.sleep(1)
            if not st["done"] and not st["nocp"]:
                continue
            r = times.flush()
            for k in total:
                total[k] += st[k]
            wait = f"  {st['nocp']} waited for a first checkpoint" if st["nocp"] else ""
            print(f"[rejoin] {st['done']:>4} processes rejoined  rejoin time p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"max {r['max']:6.1f}ms  rows from the log {st['rows']:,}  changes replayed {st['replayed']:,}  "
                  f"copies matching the writer {st['ok']}/{st['done']}  gaps {st['gaps']}{wait}", flush=True)
            print(f"[rejoin] so far: {total['done']:,} rejoins, {total['ok']:,} matching, {total['gaps']} gaps; "
                  f"{total['rows']:,} rows served from the log, 0 snapshot queries on Postgres", flush=True)
            for k in ("done", "rows", "replayed", "ok", "bad", "gaps", "nocp"):
                st[k] = 0

    await asyncio.gather(storms(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--tables", type=int, default=32, help="same as pg_writer.py")
    p.add_argument("--names", default="", help="comma-separated table names instead (a pg_bridge.py shadow run)")
    p.add_argument("--tables-each", type=int, default=3, help="tables each process subscribes to")
    p.add_argument("--storm", type=int, default=200, help="processes that start at once")
    p.add_argument("--every-s", type=float, default=30, help="seconds between storms")
    p.add_argument("--start-s", type=float, default=10, help="first storm, seconds after start")
    p.add_argument("--conns", type=int, default=8, help="connections the processes share, one rejoin in flight on each")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
