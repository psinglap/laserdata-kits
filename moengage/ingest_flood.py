"""The bulk ingestion firehose next door: enriched events on the unified topic.

Every SDK and API event, enriched with the user's profile, goes to one unified topic that
many streaming jobs read. Run this beside the Smart Trigger lane and watch trigger_eval.py:
heavy ingestion on the same broker should not move the trigger lane's p99.
"""
import argparse
import asyncio
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyExpiry, Partitioning, SendMessage

from common import DEFAULT_URL, INGEST_STREAM, UNIFIED, connect, ensure_topic


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, INGEST_STREAM, UNIFIED, a.partitions, IggyExpiry.ExpireDuration(timedelta(hours=1)))
    p = await c.producer(INGEST_STREAM, UNIFIED, topic_partitions_count=a.partitions,
                         mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=1)))
    blob = os.urandom(a.size // 2).hex().encode()       # stands in for the event plus profile enrichment
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 50))
    sent, mark = 0, time.monotonic()
    tick = time.monotonic()
    async with p:
        while True:
            head = b'{"ns":%d,"ws":' % time.time_ns()
            ms = [SendMessage(head + b'%d,"u":"u%07d","p":"' % (random.randrange(a.workspaces), random.randrange(10**7))
                              + blob + b'"}') for _ in range(per_tick)]
            # No per-key order to keep here, so one large batch per tick, partitions in turn.
            await p.send_with_partitioning(ms, Partitioning.balanced())
            sent += per_tick
            if time.monotonic() - mark >= 1:
                print(f"[ingest] {sent:,} events/s  ~{sent * a.size / 1e6:.1f} MB/s on '{UNIFIED}'", flush=True)
                sent, mark = 0, time.monotonic()
            tick = max(tick + 0.02, time.monotonic() - 0.1)
            await asyncio.sleep(max(0.0, tick - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1024, help="bytes per enriched event")
    ap.add_argument("--workspaces", type=int, default=200)
    ap.add_argument("--partitions", type=int, default=16)
    asyncio.run(main(ap.parse_args()))
