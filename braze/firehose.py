"""The event firehose next door: every raw event on its event-type topic, on the same broker.

Braze's Kafka talk describes one topic per event type, keyed by user ID, holding events for all
customers. This script writes that kind of traffic (raw events padded to --size bytes) to one
topic per event type in the 'events' stream. Run it beside the connector lane and watch
connector.py: heavy traffic on the same broker should not move the healthy connectors' p99.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import IggyExpiry, Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS, RAW_STREAM, connect, ensure_topic

TOPICS = [e.lower().replace(" ", "-") for e, _ in EVENTS]


async def main(a):
    c = await connect(a.url)
    for t in TOPICS:
        await ensure_topic(c, RAW_STREAM, t, a.partitions, IggyExpiry.ExpireDuration(timedelta(hours=1)))
    blob = os.urandom(a.size // 2).hex().encode()       # stands in for the event's properties
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 50))
    sent, mark, turn = 0, time.monotonic(), 0
    tick = time.monotonic()
    gc.freeze()
    while True:
        head = b'{"ns":%d,"app":' % time.time_ns()
        ms = [SendMessage(head + b'%d,"u":"u%08d","p":"' % (random.randrange(a.apps), random.randrange(10**8))
                          + blob + b'"}') for _ in range(per_tick)]
        # One large batch per tick, event-type topics in turn: no per-key order to keep here.
        await c.send_messages(RAW_STREAM, TOPICS[turn % len(TOPICS)], Partitioning.balanced(), ms)
        turn += 1
        sent += per_tick
        if time.monotonic() - mark >= 1:
            print(f"[firehose] {sent:,} events/s  ~{sent * a.size / 1e6:.1f} MB/s on {len(TOPICS)} event-type topics",
                  flush=True)
            sent, mark = 0, time.monotonic()
        tick = max(tick + 0.02, time.monotonic() - 0.1)
        await asyncio.sleep(max(0.0, tick - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1024, help="bytes per raw event")
    ap.add_argument("--apps", type=int, default=150)
    ap.add_argument("--partitions", type=int, default=16)
    asyncio.run(main(ap.parse_args()))
