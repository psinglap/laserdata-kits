"""A P1 domain at full volume: tag updates and embedding refreshes on their own topic.

Use it to check that a heavy, latency-tolerant domain on the same cluster does not move
the P0 lane's p99 (watch revalidate.py while this runs).
"""
import argparse
import asyncio
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, STREAM, TAGS, key, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, TAGS, topic_partitions_count=a.partitions,
                              mode=DirectProducerConfig(batch_length=200, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for an embedding vector
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 100))
    due, sent, t0 = time.monotonic(), 0, time.monotonic()
    async with p:
        while True:
            ms = [SendMessage(msg(k=key(random.randrange(a.hubs), random.randrange(2000)), kind="embedding", v=blob))
                  for _ in range(per_tick)]
            await p.send_with_partitioning(ms, Partitioning.balanced())
            sent += per_tick
            if time.monotonic() - t0 >= 1:
                print(f"[p1] {sent:,} msgs/s  ~{sent * a.size / 1e6:.0f} MB/s on '{TAGS}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=20, help="megabytes per second")
    ap.add_argument("--size", type=int, default=4096, help="bytes per message")
    ap.add_argument("--hubs", type=int, default=300)
    ap.add_argument("--partitions", type=int, default=16)
    asyncio.run(main(ap.parse_args()))
