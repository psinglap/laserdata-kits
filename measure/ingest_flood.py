"""A heavy neighbour: full ingest batches on `ingest-batch`, the topic Measure's ingest service already publishes to.

Each message is shaped like Measure's IngestBatch (batch_id, app_id, team_id, os_name, events), ~4 KB of session
events: gestures, screen views, HTTP calls, lifecycle and memory snapshots. That stream is far larger than the crash
spike lane and is read by the ingest worker's consumer group, polling 500 at a time; this script only writes it.

Use it to check that the batches on `ingest-batch` do not move the spike lane's p99 (watch spike_judge.py while
this runs). One batch per 10 ms tick, spread over partitions by the broker (balanced, as Measure's bus producer
defaults to), sized to what the tick owes, so the neighbour itself does not flood the broker with tiny requests.
Keeping the issue records on their own topic is the design rule: a surge of uploads after a release never queues
ahead of a crash spike.
"""
import argparse
import asyncio
import gc
import json
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, INGEST, PARTITIONS, STREAM, app_id, ensure_topics

EVENT_TYPES = ["gesture_click", "screen_view", "http", "lifecycle_activity", "memory_usage", "cold_launch"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, INGEST)
    p = await client.producer(STREAM, INGEST, topic_partitions_count=PARTITIONS[INGEST],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    pad = os.urandom(max(0, a.size - 900) // 2).hex()      # stands in for the events' attributes
    apps = [app_id(i) for i in range(50)]

    def batch() -> bytes:
        ev = [{"type": random.choice(EVENT_TYPES), "timestamp": time.time_ns() // 1_000_000} for _ in range(8)]
        return json.dumps({"batch_id": os.urandom(16).hex(), "app_id": random.choice(apps),
                           "team_id": "0" * 32, "os_name": random.choice(["android", "ios"]), "events": ev,
                           "attributes": pad}, separators=(",", ":")).encode()

    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[ingest-batch] starting in {a.at:g} s", flush=True)
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
                await p.send_with_partitioning([SendMessage(batch()) for _ in range(n)], Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[ingest-batch] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{INGEST}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[ingest-batch] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=20, help="megabytes per second")
    ap.add_argument("--size", type=int, default=4096, help="bytes per ingest batch")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
