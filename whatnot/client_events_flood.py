"""A heavy neighbour: the rest of client telemetry (screen views, impressions, player heartbeats), on its own topic.
Whatnot's clients and services already report events through the events ingestion service; most of that volume is
analytics that does not need a millisecond tail, and it is far larger than the story events.

Use it to check that a high-volume stream on the same broker does not move the story lane's p99 (watch
story_judge.py while this runs). One batch per 10 ms tick, spread over partitions by the broker, sized to what the
tick owes, so the neighbour itself does not flood the broker with tiny requests. Keeping story events on their own
topic is the design rule: a telemetry surge during a big show never queues ahead of a story's outcome.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import CLIENT_EVENTS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, CLIENT_EVENTS)
    p = await client.producer(STREAM, CLIENT_EVENTS, topic_partitions_count=PARTITIONS[CLIENT_EVENTS],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for an event's properties
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[client-events] starting in {a.at:g} s", flush=True)
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
                ms = [SendMessage(msg(c=f"cl-{random.randrange(10_000_000):07d}", e="screen_view", p=blob))
                      for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[client-events] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{CLIENT_EVENTS}'",
                      flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[client-events] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1024, help="bytes per event")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
