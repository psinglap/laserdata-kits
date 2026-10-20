"""A heavy neighbour: the bid-feedback log (bid, loss and impression detail for reporting) on its own topic.

Use it to check that a high-volume, latency-tolerant stream on the same broker does not move the spend
lane's p99 (watch pacing.py while this runs). One batch per 10 ms tick, spread over partitions by the
broker, so the neighbour itself does not flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, FEEDBACK, PARTITIONS, STREAM, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, FEEDBACK)
    p = await client.producer(STREAM, FEEDBACK, topic_partitions_count=PARTITIONS[FEEDBACK],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for the bid request and response detail
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 100))
    if a.at:
        print(f"[feedback] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    due, sent, t0, end = time.monotonic(), 0, time.monotonic(), time.monotonic() + (a.duration or 1e12)
    async with p:
        while time.monotonic() < end:
            ms = [SendMessage(msg(ag=random.randrange(100_000), outcome=random.choice(("loss", "win", "no-bid")), v=blob))
                  for _ in range(per_tick)]
            await p.send_with_partitioning(ms, Partitioning.balanced())
            sent += per_tick
            if time.monotonic() - t0 >= 1:
                print(f"[feedback] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{FEEDBACK}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[feedback] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=2048, help="bytes per message")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
