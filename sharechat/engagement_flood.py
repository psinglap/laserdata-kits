"""A heavy engagement stream on the same broker: raw likes, views and shares on their own topic, beside the lane.

ShareChat aggregates post engagement counters from 370k to 440k engagement events a second
(ScyllaDB Summit 23 talk). This script plays a slice of that kind of traffic, latency-tolerant
and large, so you can check it does not move the viewed-posts lane's p99 (watch dedup.py while
it runs). It sends one batch per tick, spread across partitions, as a real bulk producer would.
"""
import argparse
import asyncio
import gc
import os
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, ENGAGEMENT, PARTITIONS, STREAM, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, ENGAGEMENT)
    p = await client.producer(STREAM, ENGAGEMENT, topic_partitions_count=PARTITIONS[ENGAGEMENT],
                              mode=DirectProducerConfig(batch_length=2000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()        # stands in for an engagement event with its context
    per_s = a.mbps * 1e6 / a.size
    await asyncio.sleep(max(0.0, a.at))
    print(f"[engagement] starting: ~{a.mbps:g} MB/s on '{ENGAGEMENT}'", flush=True)
    gc.freeze()
    due = t0 = start = last = time.monotonic()
    sent, owed = 0, 0.0
    async with p:
        while not a.duration or time.monotonic() - start < a.duration:
            now = time.monotonic()
            owed += per_s * (now - last)        # by elapsed time, so a slow send means a bigger next batch
            last, n = now, int(owed)
            owed -= n
            if n:
                await p.send_with_partitioning([SendMessage(msg(kind="like", v=blob)) for _ in range(n)],
                                               Partitioning.balanced())
            sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[engagement] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[engagement] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=20, help="megabytes per second")
    ap.add_argument("--size", type=int, default=512, help="bytes per message")
    ap.add_argument("--at", type=float, default=0, help="start this many seconds in")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run; 0 runs until stopped")
    asyncio.run(main(ap.parse_args()))
