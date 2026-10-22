"""A heavy neighbour: raw event payloads on their own topic, the shape of the archive Segment keeps for replays
(events loaded back into destinations from storage) and warehouse syncs.

Use it to check that a high-volume, latency-tolerant stream on the same broker does not move the outcomes
lane's p99 (watch alerts.py while this runs). One batch per 10 ms tick, spread over partitions by the broker,
sized to what the tick owes, so the neighbour itself does not flood the broker with tiny requests. The default
record is 5 KB, the median job payload in Segment's Centrifuge post.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import ARCHIVE, DEFAULT_URL, DESTINATIONS, PARTITIONS, STREAM, ensure_topics, key, msg, workspace_id


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, ARCHIVE)
    p = await client.producer(STREAM, ARCHIVE, topic_partitions_count=PARTITIONS[ARCHIVE],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for the event payload
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[archive] starting in {a.at:g} s", flush=True)
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
                ms = [SendMessage(msg(k=key(workspace_id(random.randrange(20_000)), random.choice(DESTINATIONS)),
                                      type=random.choice(("track", "identify", "page")), body=blob)) for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[archive] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{ARCHIVE}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[archive] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=5120, help="bytes per event record")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
