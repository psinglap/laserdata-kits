"""A heavy neighbour: a customer's log burst on the same broker, on its own topic.

Middleware's ingest team has described customer log volume rising 10x for a few minutes and then returning to
baseline (WarpStream case study, Feb 2024). This sends OTLP-sized log batches to otlp-logs at --mbps, so you
can check that a burst next door does not move the command lane's p99 (watch opsai.py while this runs). One
batch per 10 ms tick, spread over partitions by the broker, sized to what the tick owes, so the neighbour
itself does not flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, LOGS, PARTITIONS, STREAM, cluster, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, LOGS)
    p = await client.producer(STREAM, LOGS, topic_partitions_count=PARTITIONS[LOGS],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for an exported batch of log records
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[logs] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    due, sent, t0, end = time.monotonic(), 0, time.monotonic(), time.monotonic() + (a.duration or 1e12)
    owed, last = 0.0, time.monotonic()
    async with p:
        while time.monotonic() < end:
            now = time.monotonic()
            owed += per_s * (now - last)    # what this tick owes, so a slow send is made up in the next batch
            last, n = now, int(owed)
            owed -= n
            if n:
                ms = [SendMessage(msg(tenant=cluster(random.randrange(2000)), records=blob)) for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[logs] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{LOGS}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[logs] done", flush=True)       # every batch is flushed: the producer's `async with` waited for it


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=8192, help="bytes per log batch")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
    # Leave without the interpreter's teardown: dropping the SDK client's runtime there could abort the
    # process with a core dump on a CI runner.
    os._exit(0)
