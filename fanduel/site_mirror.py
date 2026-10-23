"""The in-state mirror: copies every event on the wallet lane from nj-1 to nj-2, same partition, same offset.

Runs inside the state (on your side, at either site). For each partition it reads nj-1 directly with its own
connection and writes what it read, byte for byte, to the same partition at nj-2 with another connection. It is
the only writer at nj-2, so offset N at nj-2 is offset N at nj-1: a client that fails over resumes at the offset
it already had.

Rules it enforces:
  * the state boundary: both sites must be in the lane's state (SITES in common.py), and an event accepted in
    any other state is never copied; the partition stops and says so (fail closed);
  * resume without gaps or duplicates: on start it reads how far nj-2 already is and continues from there;
  * offsets stay identical: every write's confirmed base offset at nj-2 must equal the offset read at nj-1.

On every start it writes one line to the mirror-status topic at nj-2 with nj-1's head per partition at that
moment, so readers at nj-2 can tell events that waited at nj-1 while no mirror ran from live replication.
Prints a line a second: events copied, events still waiting at nj-1, offset drift and cross-state refusals.
"""
import argparse
import asyncio
import gc
import json
import time

from apache_iggy import Consumer, PollingStrategy, SendMessage

from common import LANE, PARTITIONS, PRIMARY, PRIMARY_URL, SECOND, SECOND_URL, SITES, STATE, STATUS, STREAM, \
    connect, ensure_topic, heads, state_of


async def main(a):
    if SITES.get(a.source) != STATE or SITES.get(a.target) != STATE:
        raise SystemExit(f"[mirror] refused: {a.source} -> {a.target} would leave {STATE}")
    src, dst = await connect(a.source_url), await connect(a.target_url)
    await ensure_topic(src, LANE, PARTITIONS)                  # the sportsbook may not have created it yet
    await ensure_topic(dst, LANE, PARTITIONS)
    await ensure_topic(dst, STATUS, 1)
    start = await heads(dst)                                   # nj-2 already holds offsets 0 .. start-1
    waiting = await heads(src)                                 # what nj-1 accepted while no mirror ran
    await dst.send_messages(STREAM, STATUS, 0, [SendMessage(json.dumps(
        {"ns": time.time_ns(), "from": start, "held_to": waiting}).encode())])
    held = sum(w - s for w, s in zip(waiting, start))
    print(f"[mirror {a.source}->{a.target}] {STATE} only; resuming at nj-2's offsets {start} "
          f"({held:,} events waited at {a.source} while no mirror ran)", flush=True)
    gc.freeze()
    st = dict(copied=0, drift=0, refused=0)
    copied_to = list(start)

    async def copy(pid):
        r, w = await connect(a.source_url), await connect(a.target_url)
        me, nxt = Consumer.Single(f"mirror-{a.target}-{pid}"), start[pid]
        while True:
            ms = await r.poll_messages(STREAM, LANE, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt))
            if ms:
                first = ms[0].offset()
                if first != nxt:
                    st["drift"] += 1                          # nj-1 skipped an offset; never expected
                bad = [m.offset() for m in ms if state_of(m.payload()) != STATE]
                if bad:
                    st["refused"] += len(bad)
                    print(f"[mirror] partition {pid}: event at offset {bad[0]} was not accepted in {STATE}; "
                          f"stopping this partition, nothing after it is copied", flush=True)
                    return
                res = await w.send_messages(STREAM, LANE, pid, [SendMessage(m.payload()) for m in ms])
                conf = res.confirmations
                if conf and conf[0].base_offset != first:
                    st["drift"] += 1                          # nj-2's offset differs from nj-1's
                nxt = ms[-1].offset() + 1
                copied_to[pid] = nxt
                st["copied"] += len(ms)
                if len(ms) == a.batch:
                    continue                                    # more is waiting: read on at once
            await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            h = await heads(src)
            behind = sum(max(0, x - y) for x, y in zip(h, copied_to))
            print(f"[mirror {a.source}->{a.target}] {st['copied']:>5,} events/s copied  waiting at {a.source} "
                  f"{behind:,}  offset drift {st['drift']}  cross-state refused {st['refused']}", flush=True)
            st["copied"] = 0

    await asyncio.gather(report(), *(copy(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", default=PRIMARY)
    p.add_argument("--target", default=SECOND)
    p.add_argument("--source-url", default=PRIMARY_URL)
    p.add_argument("--target-url", default=SECOND_URL)
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
