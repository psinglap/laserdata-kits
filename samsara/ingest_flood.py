"""The ingest traffic next door: raw device uploads on the same broker as the stream lane.

Samsara's engineering culture post describes 2 trillion sensor data points a year (video, images,
temperature and GPS readings) "constantly being uploaded" and an ingestion team building on AWS
Kinesis. This script writes that kind of traffic (raw upload batches padded to --size bytes) to one
topic per device family in the 'ingest' stream. Run it beside the stream lane and watch
connector.py: heavy traffic on the same broker should not move the healthy subscriptions' p99.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import IggyExpiry, Partitioning, SendMessage

from common import DEFAULT_URL, RAW_STREAM, connect, ensure_topic

TOPICS = ["vehicle-gateways", "dash-cams", "asset-gateways", "environmental-sensors"]


async def main(a):
    c = await connect(a.url)
    for t in TOPICS:
        await ensure_topic(c, RAW_STREAM, t, a.partitions, IggyExpiry.ExpireDuration(timedelta(hours=1)))
    blob = os.urandom(a.size // 2).hex().encode()       # stands in for the upload's readings
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 50))
    sent, mark, turn = 0, time.monotonic(), 0
    tick = time.monotonic()
    gc.freeze()
    while True:
        head = b'{"ns":%d,"org":' % time.time_ns()
        ms = [SendMessage(head + b'%d,"device":"d%09d","p":"' % (random.randrange(a.orgs), random.randrange(10**9))
                          + blob + b'"}') for _ in range(per_tick)]
        # One large batch per tick, device-family topics in turn: no per-key order to keep here.
        await c.send_messages(RAW_STREAM, TOPICS[turn % len(TOPICS)], Partitioning.balanced(), ms)
        turn += 1
        sent += per_tick
        if time.monotonic() - mark >= 1:
            print(f"[ingest] {sent:,} uploads/s  ~{sent * a.size / 1e6:.1f} MB/s on {len(TOPICS)} device-family topics",
                  flush=True)
            sent, mark = 0, time.monotonic()
        tick = max(tick + 0.02, time.monotonic() - 0.1)
        await asyncio.sleep(max(0.0, tick - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1024, help="bytes per raw upload")
    ap.add_argument("--orgs", type=int, default=200)
    ap.add_argument("--partitions", type=int, default=16)
    asyncio.run(main(ap.parse_args()))
