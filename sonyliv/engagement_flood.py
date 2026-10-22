"""A heavy engagement stream on the same broker: playback and app events on their own topic.

Sony LIV's analytics pipeline carries playback failures, user interactions and live sports engagement
signals on its real-time path, and high-volume events on a batch path (AWS Big Data Blog, Sep 2026), with
"billions of telemetry events per day" (ClickHouse, Aug 2026). This script plays a slice of that kind of
traffic, bulky and latency-tolerant, so you can check it does not move the answer lane's p99 (watch
scorer.py while it runs). It sends one batch per tick, spread across partitions, as a real bulk producer would.
"""
import argparse
import asyncio
import gc
import os
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import ENGAGE, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, ENGAGE)
    p = await client.producer(STREAM, ENGAGE, topic_partitions_count=PARTITIONS[ENGAGE],
                              mode=DirectProducerConfig(batch_length=2000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()        # stands in for an event with its device and session context
    per_s = a.mbps * 1e6 / a.size
    await asyncio.sleep(max(0.0, a.at))
    print(f"[engagement] starting: ~{a.mbps:g} MB/s on '{ENGAGE}'", flush=True)
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
                await p.send_with_partitioning([SendMessage(msg(kind="engagement", v=blob)) for _ in range(n)],
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
    ap.add_argument("--size", type=int, default=600, help="bytes per message")
    ap.add_argument("--at", type=float, default=0, help="start this many seconds in")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run; 0 runs until stopped")
    asyncio.run(main(ap.parse_args()))
