"""Ride history, fed by the reconnect backlog.

When a scooter comes back online, its Cloud Connect module flushes what it saved while it was offline. The
ingest step puts every event older than common.FRESH_S on reconnect-backlog instead of the live lane, so the
flush never queues ahead of live events. This reader files that backlog into each scooter's history (ride
stats, charging history, the app's timeline) at its own pace, in sequence order per scooter, dropping events a
scooter resent because the acknowledgement was lost.

Reads every partition directly from its own position, storing it on the broker at most once a second, so a
restarted reader resumes where it stopped. Prints, every second, what it filed, how far behind the newest
backlog event it is, and running totals that ci.sh checks against what scooters.py sent.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import BACKLOG, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, load


async def main(a):
    c = IggyClient.from_connection_string(a.url)
    await c.connect()
    await ensure_topics(c, BACKLOG)
    seq = {}                                # scooter -> last sequence filed
    stats, total = defaultdict(float), defaultdict(int)
    gc.freeze()

    def on_event(m, now):
        d = load(m.payload())
        stats["lag"] = max(stats["lag"], (now - d["ns"]) / 1e9)
        stats["oldest"] = max(stats["oldest"], (now - d["vt"]) / 60e9)
        if d["seq"] <= seq.get(d["s"], 0):
            stats["repeats"] += 1
            total["repeats"] += 1
            return
        seq[d["s"]] = d["seq"]
        stats["filed"] += 1
        total["filed"] += 1

    async def read():
        who = {p: Consumer.Single(f"{a.name}-p{p}") for p in range(PARTITIONS[BACKLOG])}
        nxt, committed = {}, {}
        while True:
            more = False
            for p in who:
                commit = time.monotonic() - committed.get(p, 0) >= 1
                ms = await c.poll_messages(STREAM, BACKLOG, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=commit,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.Next())
                if ms:
                    if commit:
                        committed[p] = time.monotonic()
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_event(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            print(f"[{a.name}] filed {int(stats['filed']):,}/s  repeats dropped {int(stats['repeats']):,}  "
                  f"behind the newest by {stats['lag']:.2f} s  oldest event {stats['oldest']:.1f} min old  "
                  f"scooters with history {len(seq):,}  total filed {total['filed']:,} repeats {total['repeats']:,}",
                  flush=True)
            stats.clear()

    await asyncio.gather(read(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="ride-history")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--poll-ms", type=float, default=20, help="wait between rounds when nothing more is waiting")
    asyncio.run(main(p.parse_args()))
