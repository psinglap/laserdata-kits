"""A sale-day campaign: personalised promotional sends, on their own topic beside the lane.

Meecom fetches and personalises 170 million recipients in under 10 minutes. This script
plays that burst: --recipients personalised messages over --minutes, with the published
channel mix (mostly push, then WhatsApp, SMS and email), starting --at seconds in.
Watch meecom_sender.py while it runs: order updates should keep their p99 while the
campaign queues up behind its own rate limit.

One Python process tops out well below 283,000 sends/s; it prints the rate it reached.
Run several with --share to split the campaign between them.
"""
import argparse
import asyncio
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import CAMPAIGN, CHANNELS, DEFAULT_URL, PARTITIONS, STREAM, channel, ensure_topics


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, CAMPAIGN)
    parts = PARTITIONS[CAMPAIGN]
    p = await client.producer(STREAM, CAMPAIGN, topic_partitions_count=parts,
                              mode=DirectProducerConfig(batch_length=2000, linger_time=timedelta(milliseconds=1)))
    body = os.urandom(a.size // 2).hex()              # stands in for the merged template and user data
    tails = {ch: b'","ch":"%s","v":"%s"}' % (ch.encode(), body.encode()) for ch, _ in CHANNELS}
    per_s = a.recipients / (a.minutes * 60) / a.share
    total = int(a.recipients / a.share)
    await asyncio.sleep(max(0.0, a.at))
    print(f"[campaign] starting: {total:,} recipients at up to {per_s:,.0f} sends/s", flush=True)
    sent = second = 0
    tick = mark = start = time.monotonic()
    owed = 0.0
    async with p:
        while sent < total:
            owed += per_s / 50
            n = min(int(owed), total - sent)
            owed -= n
            if n:
                # Same shape as msg(), built cheaply: one timestamp per tick, only the recipient and channel vary.
                head = b'{"ns":%d,"tpl":"sale-day","r":"U' % time.time_ns()
                ms = [SendMessage(head + b"%09d" % random.randrange(10**9) + tails[channel()]) for _ in range(n)]
                chunks = [ms[i::parts] for i in range(parts)]        # spread over partitions, sent at once
                await asyncio.gather(*(p.send_with_partitioning(c, Partitioning.partition_id(pid))
                                       for pid, c in enumerate(chunks) if c))
                sent += n
                second += n
            if time.monotonic() - mark >= 1:
                print(f"[campaign] {second:>7,} sends/s  queued {sent:,} of {total:,}  "
                      f"~{second * a.size / 1e6:.1f} MB/s on '{CAMPAIGN}'", flush=True)
                second, mark = 0, time.monotonic()
            tick = max(tick + 0.02, time.monotonic() - 0.1)
            await asyncio.sleep(max(0.0, tick - time.monotonic()))
    took = time.monotonic() - start
    print(f"[campaign] done: {sent:,} sends queued in {took:.1f} s ({sent / took:,.0f}/s); "
          f"one process at this rate needs {170e6 / (sent / took) / 60:,.1f} min for 170M recipients", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--recipients", type=float, default=170e6, help="recipients in the campaign")
    ap.add_argument("--minutes", type=float, default=10, help="time to queue every recipient")
    ap.add_argument("--share", type=int, default=1, help="number of campaign.py processes splitting the work")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--size", type=int, default=600, help="bytes per personalised message")
    asyncio.run(main(ap.parse_args()))
