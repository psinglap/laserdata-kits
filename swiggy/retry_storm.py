"""A retry storm next door: order-fulfilment events on their own topic, then 100x the ingress.

Swiggy's #BehindTheBug post describes a few partitions going offline for 4 minutes, producers
retrying, and a 100X increase in overall ingress to the cluster. This script plays the
neighbour: a steady order-events stream on the same broker, then --x times that rate for
--for seconds. Watch track.py while it runs; the ping lane's p99 should barely move.
"""
import argparse
import asyncio
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, STORM, STREAM, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, STORM, topic_partitions_count=a.partitions,
                              mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(milliseconds=2)))
    body = os.urandom(a.size // 2).hex()          # stands in for an order event with its payload
    t0, sent, mark = time.monotonic(), 0, time.monotonic()
    async with p:
        while True:
            t = time.monotonic() - t0
            x = a.x if a.at <= t < a.at + a.for_s else 1
            per_tick = max(1, int(a.mbps * x * 1e6 / a.size / 100))
            tick = time.monotonic()
            ms = [SendMessage(msg(order=f"O{random.randrange(10**9):09d}", ev="order.confirm", v=body)) for _ in range(per_tick)]
            await p.send_with_partitioning(ms, Partitioning.balanced())
            sent += per_tick
            if time.monotonic() - mark >= 1:
                state = f"storm {x:g}x" if x > 1 else "steady"
                print(f"[storm] {state}  {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{STORM}'", flush=True)
                sent, mark = 0, time.monotonic()
            await asyncio.sleep(max(0.0, 0.01 - (time.monotonic() - tick)))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=0.5, help="steady ingress, megabytes per second")
    ap.add_argument("--x", type=float, default=100, help="ingress multiplier during the storm")
    ap.add_argument("--at", type=float, default=30, help="seconds before the storm starts")
    ap.add_argument("--for", dest="for_s", type=float, default=60, help="storm length in seconds")
    ap.add_argument("--size", type=int, default=1024, help="bytes per message")
    ap.add_argument("--partitions", type=int, default=16)
    asyncio.run(main(ap.parse_args()))
