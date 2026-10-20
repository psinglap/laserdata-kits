"""A heavy neighbour on the same broker: client analytics events, on their own topic.

DoorDash's real-time events platform carries mobile and web analytics events alongside service
events, on the way to the data lake and warehouse. This script plays that neighbour: event-sized
messages at --mbps, optionally only from --at for --for seconds. Watch dispatch.py while it runs;
the store feedback lane's p99 should barely move.
"""
import argparse
import asyncio
import os
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import ANALYTICS, DEFAULT_URL, STREAM, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, ANALYTICS, topic_partitions_count=a.partitions,
                              mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(milliseconds=1)))
    body = os.urandom(a.size // 2).hex()          # stands in for a client analytics event
    rate = a.mbps * 1e6 / a.size                  # messages per second while the flood runs
    t0 = due = mark = time.monotonic()
    sent, owed = 0, 0.0
    async with p:
        while True:
            t = time.monotonic() - t0
            on = not a.at or a.at <= t < a.at + a.for_s
            if on:
                owed += rate / 100
                n, owed = int(owed), owed - int(owed)
                chunks = [[SendMessage(msg(kind="client.analytics", body=body)) for _ in range(n // a.partitions + (i < n % a.partitions))]
                          for i in range(a.partitions)]
                # One batch per partition, all sent at once, so the flood is not limited by one round trip per tick.
                await asyncio.gather(*(p.send_with_partitioning(ms, Partitioning.partition_id(i)) for i, ms in enumerate(chunks) if ms))
                sent += n
            if time.monotonic() - mark >= 1:
                state = "analytics on" if on else "idle"
                print(f"[analytics] {state}  {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{ANALYTICS}'", flush=True)
                sent, mark = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second while the flood runs")
    ap.add_argument("--size", type=int, default=1024, help="bytes per message")
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--at", type=float, default=0, help="start after this many seconds (0 = from the start)")
    ap.add_argument("--for", dest="for_s", type=float, default=60, help="seconds the flood runs, when --at is set")
    asyncio.run(main(ap.parse_args()))
