"""A heavy neighbour: a cell's backlog drained after it recovers, on its own topic on the same broker.

When a cell's Kafka comes back, the telemetry it queued is pushed through as fast as the pipeline allows. Each
record here stands in for one batch of that telemetry: big, steady, and read by the pipeline, not by the
watermark service. Use it to check that this volume next door does not move the time-wave lane's p99 (watch
watermark.py while this runs). The watermark service never reads this topic, so it never has to skip past
telemetry it does not need; that is the design rule this test holds the broker to.

One batch per 10 ms tick, spread over partitions by the broker, sized to what the tick owes, so the
neighbour itself does not flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, DRAIN, PARTITIONS, PIPELINES, STREAM, cell, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, DRAIN)
    p = await client.producer(STREAM, DRAIN, topic_partitions_count=PARTITIONS[DRAIN],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for one batch of queued telemetry
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[drain] starting in {a.at:g} s", flush=True)
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
                ms = [SendMessage(msg(cell=cell(a.cell), pipeline=random.choice(PIPELINES), points=a.points, batch=blob))
                      for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[drain] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{DRAIN}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[drain] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=100, help="megabytes per second")
    ap.add_argument("--size", type=int, default=16384, help="bytes per telemetry batch record")
    ap.add_argument("--points", type=int, default=100, help="data points per batch (a label on each record)")
    ap.add_argument("--cell", type=int, default=0, help="the recovering cell (a label on each record)")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
