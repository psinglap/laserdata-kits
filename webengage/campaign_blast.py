"""The one-time campaign send next door: a large push and email campaign going out on the same broker.

WebEngage says it sends 60B+ messages a month (webengage.com), about 23,000 a second on average,
and one-time campaigns to a whole segment arrive in bursts well above that. This script writes that
kind of traffic (one message per user, padded to --size bytes like a push or email payload plus
metadata) to a campaign-sends topic in its own stream. Run it beside the trip lane and watch
journey_engine.py: a campaign blast on the same broker should not move the journeys' p99.
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
            head = b'{"ns":%d,"campaign":"one-time-%d",' % (time.time_ns(), random.randrange(a.campaigns))
            ms = [SendMessage(head + b'"u":"u%09d","body":"' % random.randrange(10**9) + body + b'"}')
                  for _ in range(per_tick)]
            # One large batch per tick, round-robin over partitions: no per-user order to keep here.
            await c.send_messages(CAMPAIGN_STREAM, "campaign-sends", Partitioning.balanced(), ms)
            sent += per_tick
        if time.monotonic() - mark >= 1:
            if time.monotonic() - start >= a.at:
                print(f"[campaign] {sent:,} campaign messages/s  ~{sent * a.size / 1e6:.1f} MB/s on campaign-sends",
                      flush=True)
            sent, mark = 0, time.monotonic()
        tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)
        await asyncio.sleep(max(0.0, tick - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--rate", type=float, default=50_000, help="campaign messages a second (about 2x the monthly average)")
    ap.add_argument("--size", type=int, default=400, help="bytes per campaign message")
    ap.add_argument("--campaigns", type=int, default=900)
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--at", type=float, default=0, help="seconds after start the blast begins")
    ap.add_argument("--tick-ms", type=float, default=20)
    asyncio.run(main(ap.parse_args()))
