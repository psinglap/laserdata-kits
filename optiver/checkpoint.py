"""Checkpoints: the full state of every table, written back to the log every --every-s seconds.

Reads the change log like any subscriber (direct partition reads), keeps every table's rows, and every
--every-s writes one checkpoint per partition to `table-checkpoints`, on the same partition number:
the rows, sequence and state hash of each of that partition's tables, plus the change-log offset the
checkpoint covers. A process that starts or restarts reads the latest checkpoint for its tables'
partitions and replays only the changes after that offset (rejoin.py).

It is not on the live path: the writer never waits for it, and a late checkpoint only means a longer
replay. Checkpoints are sent from their own task and connection, so the reads never wait on a send.
"""
import argparse
import asyncio
import gc
import json
import time

from apache_iggy import Consumer, PollingStrategy, SendMessage

from common import CHANGES, CHECKPOINTS, DEFAULT_URL, STREAM, Table, connect, wait_for_topics


async def main(a):
    admin = await connect(a.url)
    parts = await wait_for_topics(admin, "checkpoint")
    state = [dict() for _ in range(parts)]          # partition -> table -> Table
    offset = [None] * parts                         # partition -> last change-log offset applied
    st = dict(applied=0, bad=0, gaps=0, written=0, bytes=0)
    gc.freeze()

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"checkpoint-{pid}"), None
        while True:
            ms = await c.poll_messages(STREAM, CHANGES, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else PollingStrategy.First())
            for m in ms:
                ch = json.loads(m.payload())
                tab = state[pid].get(ch["t"])
                if tab is None:
                    tab = state[pid][ch["t"]] = Table()
                res = tab.apply(ch)
                st["applied"] += res != "repeat"
                st["gaps"] += res.startswith("gap")
                st["bad"] += res.endswith("bad")
            if ms:
                nxt = ms[-1].offset() + 1
                offset[pid] = ms[-1].offset()
                if len(ms) == a.batch:
                    continue
            await asyncio.sleep(a.poll_ms / 1000)   # not latency-measured: poll gently, leave the CPU to the lane

    async def write():
        c = await connect(a.url)
        due = time.monotonic() + a.every_s
        while True:
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            due += a.every_s                        # from the due time, not from now
            for pid in range(parts):
                if offset[pid] is None:
                    continue
                cp = {"p": pid, "off": offset[pid], "n": time.time_ns(),
                      "tables": {t: {"s": x.seq, "h": x.h, "rows": list(x.rows.items())} for t, x in state[pid].items()}}
                body = json.dumps(cp, separators=(",", ":")).encode()
                await c.send_messages(STREAM, CHECKPOINTS, pid, [SendMessage(body)])
                st["written"] += 1
                st["bytes"] += len(body)
                await asyncio.sleep(0)              # let the readers in between partitions

    async def report():
        while True:
            await asyncio.sleep(1)
            tabs = sum(len(s) for s in state)
            print(f"[checkpoint] applied {st['applied']:,} changes/s  checkpoints written {st['written']} "
                  f"({st['bytes'] / 1e3:,.0f} kB)  tables {tabs}  hash mismatches {st['bad']}  gaps {st['gaps']}", flush=True)
            st.update(applied=0, written=0, bytes=0)

    await asyncio.gather(report(), write(), *(read(p) for p in range(parts)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--every-s", type=float, default=5, help="seconds between checkpoints")
    p.add_argument("--poll-ms", type=float, default=20)
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
