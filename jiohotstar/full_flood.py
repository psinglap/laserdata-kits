"""Full heartbeats on the same broker: the ~7.5 KB heartbeat on its own topic, beside the lane.

JioHotstar's Oct 2026 heartbeat post: every active playback session emits a ~7.5 KB protobuf
heartbeat with 300+ fields, at 2.1 million messages a second, and some consumers (the full-data
consumers) still read all of it. This script plays that kind of traffic, large and
latency-tolerant, so you can check it does not move the lane's p99 (watch qos.py and routing.py
while it runs). It sends one batch per tick, spread across partitions, as a real bulk producer would.
"""
import argparse
import asyncio
import gc
import os
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, FULL, PARTITIONS, STREAM, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, FULL)
    p = await client.producer(STREAM, FULL, topic_partitions_count=PARTITIONS[FULL],
                              mode=DirectProducerConfig(batch_length=500, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()        # stands in for the full session context
    per_s = a.mbps * 1e6 / a.size
    await asyncio.sleep(max(0.0, a.at))
    print(f"[full] starting: ~{a.mbps:g} MB/s of full heartbeats on '{FULL}'", flush=True)
    gc.freeze()
    t0 = start = last = due = time.monotonic()
    sent, owed = 0, 0.0
    async with p:
        while not a.duration or time.monotonic() - start < a.duration:
            now = time.monotonic()
            owed += per_s * (now - last)        # by elapsed time, so a slow send means a bigger next batch
            last, n = now, int(owed)
            owed -= n
            if n:
                await p.send_with_partitioning([SendMessage(msg(kind="heartbeat", v=blob)) for _ in range(n)],
                                               Partitioning.balanced())
            sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[full] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[full] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=20, help="megabytes per second")
    ap.add_argument("--size", type=int, default=7500, help="bytes per message")
    ap.add_argument("--at", type=float, default=0, help="start this many seconds in")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run; 0 runs until stopped")
    asyncio.run(main(ap.parse_args()))
