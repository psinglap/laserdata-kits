"""A log burst next door: application log lines on their own topic, then --x times the ingress.

Porter ships about 500 TB of application logs a month with a self-hosted Vector collector
(Google Cloud case study). This script plays that neighbour on the same broker: JSON log lines
from a handful of services at a steady rate, then --x times that rate for --for seconds, like a
noisy deploy or an incident that turns on debug logging. Watch trips.py while it runs; the
driver ping lane's p99 should barely move.
"""
import argparse
import asyncio
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, LOGS, STREAM, msg

SERVICES = ["order-service", "allocation", "pricing", "driver-app-gateway", "payments", "notifications"]
LEVELS = ["info"] * 8 + ["warn", "error"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, LOGS, topic_partitions_count=a.partitions,
                              mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=100)))
    body = os.urandom(a.size // 2).hex()          # stands in for the log line and its fields
    t0 = due = mark = time.monotonic()
    sent = 0
    async with p:
        while True:
            t = time.monotonic() - t0
            x = a.x if a.at <= t < a.at + a.for_s else 1
            per_tick = max(1, int(a.mbps * x * 1e6 / a.size / 50))
            ms = [SendMessage(msg(svc=random.choice(SERVICES), level=random.choice(LEVELS), pod=f"pod-{random.randrange(400)}",
                                  line=body)) for _ in range(per_tick)]
            # spread the tick over partitions and send the chunks at once, so a burst is not paced by one round trip
            chunks = [ms[i::a.partitions] for i in range(a.partitions)]
            await asyncio.gather(*(p.send_with_partitioning(c, Partitioning.partition_id(pid))
                                   for pid, c in enumerate(chunks) if c))
            sent += per_tick
            if time.monotonic() - mark >= 1:
                state = f"burst {x:g}x" if x > 1 else "steady"
                print(f"[logs] {state}  {sent:,} lines/s  ~{sent * a.size / 1e6:.1f} MB/s on '{LOGS}'", flush=True)
                sent, mark = 0, time.monotonic()
            due = max(due + 0.02, time.monotonic() - 0.1)        # never bank more than 100 ms of catch-up
            await asyncio.sleep(max(0.0, due - time.monotonic()))   # behind schedule: send back to back


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=0.5, help="steady ingress, megabytes per second")
    ap.add_argument("--x", type=float, default=20, help="ingress multiplier during the burst")
    ap.add_argument("--at", type=float, default=30, help="seconds before the burst starts")
    ap.add_argument("--for", dest="for_s", type=float, default=60, help="burst length in seconds")
    ap.add_argument("--size", type=int, default=1024, help="bytes per log line")
    ap.add_argument("--partitions", type=int, default=16)
    asyncio.run(main(ap.parse_args()))
