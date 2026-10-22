"""A heavy neighbour: bid request, bid response and win logs on their own topic, the shape of the event and
reporting pipelines behind the RTB services (campaign, supply and spend reports).

Use it to check that a high-volume, latency-tolerant stream on the same broker does not move the click-install
lane's p99 (watch ctit_scorer.py while this runs). One batch per 10 ms tick, spread over partitions by the
broker, sized to what the tick owes, so the neighbour itself does not flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import BIDLOGS, DEFAULT_URL, PARTITIONS, STREAM, device, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, BIDLOGS)
    p = await client.producer(STREAM, BIDLOGS, topic_partitions_count=PARTITIONS[BIDLOGS],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for the OpenRTB request, response and win fields
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[bidlogs] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    due, sent, t0, end = time.monotonic(), 0, time.monotonic(), time.monotonic() + (a.duration or 1e12)
    owed, last = 0.0, time.monotonic()
    async with p:
        while time.monotonic() < end:
            now = time.monotonic()
            owed += per_s * (now - last)    # what this tick owes, so a slow send is made up in the next batch
            last, n = now, int(owed)
            owed -= n
            if n:
                ms = [SendMessage(msg(d=device(random.randrange(1_000_000)), kind=random.choice(("bid", "no-bid", "win")),
                                      body=blob)) for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[bidlogs] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{BIDLOGS}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[bidlogs] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=2048, help="bytes per log record")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
