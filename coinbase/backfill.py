"""A heavy neighbour on the same lane: a new indexer backfilling, reading every chain's events from the first one
as fast as --rate allows, rebuilding the canonical chains, then starting again from the first event.

ChainStorage's README recommends backfilling with the batch APIs and then switching to the event stream. On the
lane, a backfill is the same read from offset 0, so it is the read pattern to test: use it to check that a big
replay on the same broker does not move the live indexers' p99 (watch indexer.py while this runs).
"""
import argparse
import asyncio
import gc
import os
import time

from apache_iggy import Consumer, PollingStrategy

from common import CHAINS, DEFAULT_URL, EVENTS, PARTITIONS, STREAM, Canon, connect, ensure_topics, load


async def main(a):
    c = await connect(a.url)
    await ensure_topics(c)
    who = Consumer.Single(f"backfill.{os.getpid()}")
    if a.at:
        print(f"[backfill] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    end = time.monotonic() + (a.duration or 1e12)
    passes, t_print, n_sec = 0, time.monotonic(), 0
    while time.monotonic() < end:
        canon = {ch: Canon() for ch in CHAINS}
        nxt = [0] * PARTITIONS
        done = [False] * PARTITIONS
        t0, n = time.monotonic(), 0
        while not all(done) and time.monotonic() < end:
            for p in range(PARTITIONS):
                if done[p]:
                    continue
                ms = await c.poll_messages(STREAM, EVENTS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False, polling_strategy=PollingStrategy.Offset(nxt[p]))
                if len(ms) < a.batch:
                    done[p] = True         # reached the live tip of this partition
                for m in ms:
                    e = load(m.payload())
                    canon[e["chain"]].apply(e)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                n += len(ms)
                n_sec += len(ms)
                # pace the replay to --rate events a second
                ahead = n / a.rate - (time.monotonic() - t0)
                if ahead > 0:
                    await asyncio.sleep(ahead)
            if time.monotonic() - t_print >= 1:
                print(f"[backfill] {n_sec:,} events/s replayed from the log", flush=True)
                n_sec, t_print = 0, time.monotonic()
        if all(done):
            passes += 1
            gaps = sum(x.gaps for x in canon.values())
            breaks = sum(x.breaks for x in canon.values())
            print(f"[backfill] pass {passes}: rebuilt {sum(bool(x.blocks) for x in canon.values())} chains from "
                  f"{n:,} events in {time.monotonic() - t0:.1f}s  gaps {gaps}  breaks {breaks}", flush=True)
    print("[backfill] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--rate", type=float, default=20_000, help="events a second to replay")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
