"""A heavy neighbour: readings buffered while gateways were out of coverage, uploaded when they reconnect,
on their own topic. Motive documents a path that keeps environmental data when a vehicle loses cellular
connectivity; whatever carries it, those uploads are bulky, bursty and not urgent.

Use it to check that a high-volume, latency-tolerant stream on the same broker does not move the cargo sensor
lane's p99 (watch alert_rules.py while this runs). One batch per 10 ms tick, spread over partitions by the
broker, sized to what the tick owes, so the neighbour itself does not flood the broker with tiny requests.
Keeping backfill on its own topic is the design rule: a reconnect storm never queues ahead of live door events.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import BACKFILL, DEFAULT_URL, PARTITIONS, STREAM, asset, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, BACKFILL)
    p = await client.producer(STREAM, BACKFILL, topic_partitions_count=PARTITIONS[BACKFILL],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for a run of buffered one-minute readings
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[backfill] starting in {a.at:g} s", flush=True)
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
                ms = [SendMessage(msg(a=asset(random.randrange(1_000_000)), minutes=a.size // 64, readings=blob))
                      for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[backfill] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{BACKFILL}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[backfill] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=8192, help="bytes per upload record")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
