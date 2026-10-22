"""A log burst next door: application log lines on their own topic, then --x times the rate.

redBus's tracking post (Feb 2022) puts about 100K log messages a minute on the same Kafka cluster as the 30K
process state updates a minute: most of the cluster's traffic is logs. This script plays that neighbour on the
same broker: JSON log lines from the tracking consumers at a steady rate, then --x times that rate for --for
seconds, like a bad deploy or an incident with debug logging on. Watch tracker.py while it runs; the GPS
lane's p99 should barely move.
"""
import argparse
import asyncio
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, LOGS, PARTITIONS, STREAM, ensure_topics, msg

CONSUMERS = ["assignment-validator", "gps-mapper", "bp-detection", "passenger-bp-detection", "notification-dispatcher",
             "booking-extension", "message-scheduler", "gps-scanner"]
LEVELS = ["info"] * 8 + ["warn", "error"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, LOGS)
    p = await client.producer(STREAM, LOGS, topic_partitions_count=PARTITIONS[LOGS],
                              mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=100)))
    body = os.urandom(a.size // 2).hex()          # stands in for the log line and its fields
    t0 = due = mark = time.monotonic()
    sent = 0
    async with p:
        while True:
            t = time.monotonic() - t0
            x = a.x if a.at <= t < a.at + a.for_s else 1
            per_tick = max(1, int(a.lines * x / 50))
            ms = [SendMessage(msg(svc=random.choice(CONSUMERS), level=random.choice(LEVELS),
                                  pod=f"pod-{random.randrange(40)}", line=body)) for _ in range(per_tick)]
            # one batch per tick across the partitions (balanced), so the neighbour is busy, not chatty
            await p.send_with_partitioning(ms, Partitioning.balanced())
            sent += per_tick
            if time.monotonic() - mark >= 1:
                state = f"burst {x:g}x" if x > 1 else "steady"
                print(f"[logs] {state}  {sent:,} lines/s  ~{sent * a.size / 1e6:.1f} MB/s on '{LOGS}'", flush=True)
                sent, mark = 0, time.monotonic()
            due = max(due + 0.02, time.monotonic() - 0.1)        # never bank more than 100 ms of catch-up
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--lines", type=float, default=1700, help="steady log lines per second (about 100K a minute)")
    ap.add_argument("--x", type=float, default=10, help="rate multiplier during the burst")
    ap.add_argument("--at", type=float, default=30, help="seconds before the burst starts")
    ap.add_argument("--for", dest="for_s", type=float, default=60, help="burst length in seconds")
    ap.add_argument("--size", type=int, default=400, help="bytes per log line")
    asyncio.run(main(ap.parse_args()))
