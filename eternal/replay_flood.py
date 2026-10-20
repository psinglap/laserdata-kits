"""A heavy neighbour on the same broker: a benchmark replay, on its own topic.

Eternal's backend services record live traffic requests into Kafka and replay them later for
load tests; a backfill averages 52 million messages a minute (WarpStream case study, Aug 2026).
This script plays that neighbour: recorded-request-sized messages at --mbps, optionally only
from --at for --for seconds. Watch fpt.py while it runs; the kitchen lane's p99 should barely move.
"""
import argparse
import asyncio
import os
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, REPLAY, STREAM, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, REPLAY, topic_partitions_count=a.partitions,
                              mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(milliseconds=1)))
    body = os.urandom(a.size // 2).hex()          # stands in for a recorded request
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 100))
    t0 = due = mark = time.monotonic()
    sent = 0
    async with p:
        while True:
            t = time.monotonic() - t0
            on = not a.at or a.at <= t < a.at + a.for_s
            if on:
                ms = [SendMessage(msg(kind="recorded.request", body=body)) for _ in range(per_tick)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += per_tick
            if time.monotonic() - mark >= 1:
                state = "replay on" if on else "idle"
                print(f"[replay] {state}  {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{REPLAY}'", flush=True)
                sent, mark = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second while the replay runs")
    ap.add_argument("--size", type=int, default=1024, help="bytes per message")
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--at", type=float, default=0, help="start after this many seconds (0 = from the start)")
    ap.add_argument("--for", dest="for_s", type=float, default=60, help="seconds the replay runs, when --at is set")
    asyncio.run(main(ap.parse_args()))
