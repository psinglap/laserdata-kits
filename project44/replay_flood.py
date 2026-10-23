"""A heavy neighbour: a 72-hour replay on its own topic.
project44's engineers have described stress-testing their Collaborative Visibility system by rewinding its Kafka
offsets by 72 hours, so three days of data arrived at once (Kafka compression post, 2023); their topics carry
fairly large messages. A replay like that, or a backfill of EDI and document data, is bulky, bursty and not urgent.

Use it to check that a high-volume, latency-tolerant stream on the same broker does not move the position share
lane's p99 (watch share_gate.py while this runs). One batch per 10 ms tick, spread over partitions by the broker,
sized to what the tick owes, so the neighbour itself does not flood the broker with tiny requests. Keeping the
replay on its own topic is the design rule: three days of backlog never queue ahead of a live position.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import REPLAY, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg

KINDS = ["shipment-shared", "milestone", "edi-214", "ocean-event", "document-scan", "eta-snapshot", "order-link"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, REPLAY)
    p = await client.producer(STREAM, REPLAY, topic_partitions_count=PARTITIONS[REPLAY],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    body = os.urandom(a.size // 2).hex()       # stands in for a full shipment record, as the replayed topics carry
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[replay] starting in {a.at:g} s", flush=True)
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
                ms = [SendMessage(msg(kind=random.choice(KINDS), off=random.getrandbits(40), body=body)) for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[replay] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{REPLAY}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[replay] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=2048, help="bytes per replayed record")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
