"""The scheduled-campaign blast next door: Black Friday campaign texts going out on the same broker.

Attentive's engineering post says the send path must sustain "a minimum rate of around 25,000
messages per second". This script writes that kind of traffic (one campaign text per subscriber,
padded to --size bytes like an SMS body plus metadata) to a campaign-sends topic in its own
stream. Run it beside the decision lane and watch decision.py: a campaign blast on the same
broker should not move the triggered lane's p99.
"""
import argparse
import asyncio
import gc
import os
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import CAMPAIGN_STREAM, DEFAULT_URL, connect, ensure_topic


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, CAMPAIGN_STREAM, "campaign-sends", a.partitions)
    body = os.urandom(a.size // 2).hex().encode()       # stands in for the message body and its metadata
    per_tick = max(1, int(a.rate * a.tick_ms / 1000))
    sent, mark, tick = 0, time.monotonic(), time.monotonic()
    start = time.monotonic()
    gc.freeze()
    while True:
        if time.monotonic() - start >= a.at:
            head = b'{"ns":%d,"campaign":"bfcm-%d",' % (time.time_ns(), random.randrange(a.brands))
            ms = [SendMessage(head + b'"u":"s%09d","body":"' % random.randrange(10**9) + body + b'"}')
                  for _ in range(per_tick)]
            # One large batch per tick, round-robin over partitions: no per-subscriber order to keep here.
            await c.send_messages(CAMPAIGN_STREAM, "campaign-sends", Partitioning.balanced(), ms)
            sent += per_tick
        if time.monotonic() - mark >= 1:
            if time.monotonic() - start >= a.at:
                print(f"[bfcm] {sent:,} campaign texts/s  ~{sent * a.size / 1e6:.1f} MB/s on campaign-sends", flush=True)
            sent, mark = 0, time.monotonic()
        tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)
        await asyncio.sleep(max(0.0, tick - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--rate", type=float, default=25_000, help="campaign texts a second")
    ap.add_argument("--size", type=int, default=400, help="bytes per campaign text")
    ap.add_argument("--brands", type=int, default=8_000)
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--at", type=float, default=0, help="seconds after start the blast begins")
    ap.add_argument("--tick-ms", type=float, default=20)
    asyncio.run(main(ap.parse_args()))
