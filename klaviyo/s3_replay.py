"""Bulk replay next door: events recovered from S3, written in large batches to their own topic.

Klaviyo's publisher keeps events it could not write to Kafka (mostly serialization failures) in
S3, and replays them once the bug is fixed. This script plays that replay: large batches of
recovered events (--size bytes each) at --mbps to the s3-replay topic on the same broker. Run it
beside the failed-writes lane and watch retrier.py: a bulk replay on the same nodes should not move
the capture-to-retry p99.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import IggyExpiry, Partitioning, SendMessage

from common import DEFAULT_URL, REPLAY, STREAM, connect, ensure_topic


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, STREAM, REPLAY, a.partitions, IggyExpiry.ExpireDuration(timedelta(hours=1)))
    blob = os.urandom(a.size // 2).hex().encode()       # stands in for the recovered event body
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 50))
    sent, mark = 0, time.monotonic()
    tick = time.monotonic()
    gc.freeze()
    while True:
        head = b'{"ns":%d,"src":"s3","pf":' % time.time_ns()
        ms = [SendMessage(head + b'"pf%09d","body":"' % random.randrange(10**9) + blob + b'"}')
              for _ in range(per_tick)]
        # One large batch per tick: a replay has no per-profile order to keep against live traffic.
        await c.send_messages(STREAM, REPLAY, Partitioning.balanced(), ms)
        sent += per_tick
        if time.monotonic() - mark >= 1:
            print(f"[s3-replay] {sent:,} recovered events/s  ~{sent * a.size / 1e6:.1f} MB/s to '{REPLAY}'", flush=True)
            sent, mark = 0, time.monotonic()
        tick = max(tick + 0.02, time.monotonic() - 0.1)
        await asyncio.sleep(max(0.0, tick - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=40, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1500, help="bytes per recovered event")
    ap.add_argument("--partitions", type=int, default=8)
    asyncio.run(main(ap.parse_args()))
