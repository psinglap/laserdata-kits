"""A subscriber node: keeps a live copy of the tables and serves --procs simulated processes on this host.

Reads every partition of the change log with its own connection and its own offsets (direct partition
reads, no consumer group), applies each change to its copy of the table, and checks the copy against
the writer's state hash after every change. Each simulated process subscribes to --tables-each tables,
as PG feed subscribers subscribe to "a small subset of the data"; the node counts the change deliveries
those processes would receive.

Prints a line a second:
  * commit-to-subscriber latency (commit time at the writer -> applied here), with PASS or FAIL against
    --budget-ms;
  * deliveries to the simulated processes;
  * tables matching the writer: tables whose copy has the same state hash as the database after the
    last change applied (must be all of them);
  * gaps: a missing sequence number for a table (must stay 0).
"""
import argparse
import asyncio
import gc
import json
import random
import time
from collections import Counter

from apache_iggy import Consumer, PollingStrategy

from common import CHANGES, DEFAULT_URL, STREAM, Table, Window, connect, tables, wait_for_topics


async def main(a):
    admin = await connect(a.url)
    parts = await wait_for_topics(admin, a.name)
    names = tables(a.tables)
    rng = random.Random(a.name)
    watchers = Counter()                            # table -> processes subscribed to it on this node
    for _ in range(a.procs):
        for t in rng.sample(names, min(a.tables_each, len(names))):
            watchers[t] += 1
    even = max(1, round(a.procs * a.tables_each / a.tables))   # for tables named elsewhere (pg_bridge.py)
    copy = {}                                       # table -> Table
    bad = set()                                     # tables whose copy no longer matches the writer's hash
    lat, st = Window(), dict(live=0, deliveries=0, gaps=0, replayed=0)
    started, caught, live_parts = time.monotonic(), [None], set()
    budget_ns = a.budget_ms * 1_000_000
    gc.freeze()

    def apply(pid, ms, now):
        for m in ms:
            c = json.loads(m.payload())
            t = c["t"]
            tab = copy.get(t)
            if tab is None:
                tab = copy[t] = Table()
                watchers.setdefault(t, even)
            res = tab.apply(c)
            if res == "repeat":
                continue
            if res.startswith("gap"):
                st["gaps"] += 1
            if res.endswith("bad"):
                bad.add(t)
            else:
                bad.discard(t)
            if c["o"] == "S":
                continue                            # the startup state is not a live change
            if pid not in live_parts:
                if now - c["n"] >= budget_ns:
                    st["replayed"] += 1             # written before this node was reading: catch-up, not live
                    continue
                live_parts.add(pid)                 # this partition is at the live edge: time its changes from here
                if caught[0] is None and len(live_parts) == parts:
                    caught[0] = time.monotonic()
            lat.add((now - c["n"]) / 1e6)
            st["live"] += 1
            st["deliveries"] += watchers[t]

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        while True:
            ms = await c.poll_messages(STREAM, CHANGES, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else PollingStrategy.First())
            if ms:
                apply(pid, ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                        # more is waiting (catch-up or a burst): read on at once
            await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        told = False
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] catching up: {st['replayed']:,} changes replayed, {len(copy)} tables, "
                      f"{len(live_parts)} of {parts} partitions live", flush=True)
                lat.flush()
                continue
            if not told:
                told = True
                rows = sum(len(x.rows) for x in copy.values())
                print(f"[{a.name}] caught up in {caught[0] - started:.2f}s: {len(copy)} tables, {rows:,} rows, "
                      f"{st['replayed']:,} changes replayed", flush=True)
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {st['live']:>6,} changes/s  commit-to-subscriber p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  deliveries to {a.procs:,} processes "
                  f"{st['deliveries']:,}/s  tables matching the writer {len(copy) - len(bad)}/{len(copy)}  gaps {st['gaps']}",
                  flush=True)
            st.update(live=0, deliveries=0)

    await asyncio.gather(report(), *(read(p) for p in range(parts)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="sub-1")
    p.add_argument("--tables", type=int, default=32, help="same as pg_writer.py")
    p.add_argument("--procs", type=int, default=2000, help="simulated subscriber processes on this node")
    p.add_argument("--tables-each", type=int, default=3, help="tables each process subscribes to")
    p.add_argument("--budget-ms", type=float, default=10, help="p99 budget, commit to subscriber")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=2000)
    asyncio.run(main(p.parse_args()))
