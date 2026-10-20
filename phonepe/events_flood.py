"""A heavy app-events stream on the same broker: bulk analytics events on their own topic, beside the lane.

PhonePe's Kafka carries around a trillion messages a day, and PhonePe's blog says about 95% of
use cases take the spooled path, with analytics p99 under 3 s. This script plays a slice of that
kind of traffic, latency-tolerant and large, so you can check it does not move
the velocity lane's p99 (watch risk.py while it runs). It sends one batch per tick,
spread across partitions, as a real bulk producer would.
"""
import argparse
import asyncio
import gc
import os
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import EVENTS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, EVENTS)
    p = await client.producer(STREAM, EVENTS, topic_partitions_count=PARTITIONS[EVENTS],
                              mode=DirectProducerConfig(batch_length=2000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()        # stands in for an app or backend analytics event
    per_s = a.mbps * 1e6 / a.size
    await asyncio.sleep(max(0.0, a.at))
    print(f"[events] starting: ~{a.mbps:g} MB/s on '{EVENTS}'", flush=True)
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
                await p.send_with_partitioning([SendMessage(msg(kind="event", v=blob)) for _ in range(n)],
                                               Partitioning.balanced())
            sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[events] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[events] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=20, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1024, help="bytes per message")
    ap.add_argument("--at", type=float, default=0, help="start this many seconds in")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run; 0 runs until stopped")
    asyncio.run(main(ap.parse_args()))
