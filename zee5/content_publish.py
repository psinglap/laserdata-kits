"""A release drop next door: content metadata on its own topic, then --x times the rate.

ZEE5's CMS publishes over Kafka whenever a title is published or its title, description,
subtitles or languages change (Zee's then CTO, Frontier Enterprise, Aug 2024).
This script plays that neighbour on the same broker: metadata records at a steady rate, then
--x times that rate for --for seconds, like a big release with every language and subtitle
track landing at once. Watch continue_watching.py while it runs; the heartbeat lane's p99
should barely move.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, METADATA, STREAM, msg

CHANGES = ["published", "title", "description", "subtitle_added", "audio_language_added", "artwork"]
LANGS = ["hi", "ta", "te", "kn", "ml", "bn", "mr", "gu", "pa", "or", "en"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, METADATA, topic_partitions_count=a.partitions,
                              mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=100)))
    body = os.urandom(a.size // 2).hex()          # stands in for the description, cast, tags and artwork URLs
    gc.freeze()
    t0 = due = mark = time.monotonic()
    sent = 0
    async with p:
        while True:
            t = time.monotonic() - t0
            x = a.x if a.at <= t < a.at + a.for_s else 1
            per_tick = max(1, int(a.rate * x / 50))
            ms = [SendMessage(msg(asset=f"T{random.randrange(60000):05d}", change=random.choice(CHANGES),
                                  lang=random.choice(LANGS), body=body)) for _ in range(per_tick)]
            # one batch per tick, round-robin over partitions: a busy neighbour without thousands of tiny requests
            await p.send_with_partitioning(ms, Partitioning.balanced())
            sent += per_tick
            if time.monotonic() - mark >= 1:
                state = f"release drop {x:g}x" if x > 1 else "steady"
                print(f"[metadata] {state}  {sent:,} records/s  ~{sent * a.size / 1e6:.1f} MB/s on '{METADATA}'", flush=True)
                sent, mark = 0, time.monotonic()
            due = max(due + 0.02, time.monotonic() - 0.1)        # never bank more than 100 ms of catch-up
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--rate", type=float, default=500, help="steady metadata records per second")
    ap.add_argument("--x", type=float, default=20, help="rate multiplier during the release drop")
    ap.add_argument("--at", type=float, default=30, help="seconds before the drop starts")
    ap.add_argument("--for", dest="for_s", type=float, default=60, help="drop length in seconds")
    ap.add_argument("--size", type=int, default=2048, help="bytes per metadata record")
    ap.add_argument("--partitions", type=int, default=8)
    asyncio.run(main(ap.parse_args()))
