"""A heavy DataPlatform stream on the same broker: bulk events on their own topic, beside the lane.

Groww's DataPlatform Kafka carries about 75 billion messages a trading day. This script plays a
slice of that kind of traffic, latency-tolerant and large, so you can check it does not move
the position lane's p99 (watch holdings.py while it runs). It sends one batch per tick,
spread across partitions, as a real bulk producer would.
"""
import argparse
import asyncio
import gc
import os
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DATAPLATFORM, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, DATAPLATFORM)
    p = await client.producer(STREAM, DATAPLATFORM, topic_partitions_count=PARTITIONS[DATAPLATFORM],
                              mode=DirectProducerConfig(batch_length=2000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()        # stands in for a clickstream or market-data record
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 100))
    await asyncio.sleep(max(0.0, a.at))
    print(f"[dataplatform] starting: ~{a.mbps:g} MB/s on '{DATAPLATFORM}'", flush=True)
    gc.freeze()
    due = t0 = start = time.monotonic()
    sent = 0
    async with p:
        while not a.duration or time.monotonic() - start < a.duration:
            await p.send_with_partitioning([SendMessage(msg(kind="event", v=blob)) for _ in range(per_tick)],
                                           Partitioning.balanced())
            sent += per_tick
            if time.monotonic() - t0 >= 1:
                print(f"[dataplatform] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[dataplatform] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=20, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1024, help="bytes per message")
    ap.add_argument("--at", type=float, default=0, help="start this many seconds in")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run; 0 runs until stopped")
    asyncio.run(main(ap.parse_args()))
