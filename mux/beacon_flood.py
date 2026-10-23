"""The ingest traffic next door: raw player beacons on the same broker as the export lane.

Mux describes the front of the Mux Data pipeline: player SDKs send playback events through load
balancers to collection servers, which "enqueue them on a Kafka cluster for asynchronous processing",
and most views "will transmit multiple beacons". This script writes that kind of traffic (beacon
batches padded to --size bytes, keyed by nothing in particular) to one topic per SDK family in the
'beacons' stream. Run it beside the export lane and watch exporter.py: heavy beacon traffic on the
same broker should not move the healthy exports' p99.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import IggyExpiry, Partitioning, SendMessage

from common import BEACON_STREAM, DEFAULT_URL, connect, ensure_topic

TOPICS = ["web-sdk-beacons", "ios-sdk-beacons", "android-sdk-beacons", "tv-sdk-beacons"]
EVENTS = (b"playing", b"pause", b"rebufferstart", b"rebufferend", b"renditionchange", b"heartbeat", b"error")


async def main(a):
    c = await connect(a.url)
    for t in TOPICS:
        await ensure_topic(c, BEACON_STREAM, t, a.partitions, IggyExpiry.ExpireDuration(timedelta(hours=1)))
    blob = os.urandom(a.size // 2).hex().encode()       # stands in for the beacon's player and playback fields
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 50))
    sent, mark, turn = 0, time.monotonic(), 0
    tick = time.monotonic()
    gc.freeze()
    while True:
        head = b'{"ns":%d,"env":' % time.time_ns()
        ms = [SendMessage(head + b'%d,"e":"%s","view":"%012x","p":"' % (
            random.randrange(a.envs), random.choice(EVENTS), random.randrange(16**12)) + blob + b'"}')
            for _ in range(per_tick)]
        # One large batch per tick, SDK-family topics in turn: no per-key order to keep here.
        await c.send_messages(BEACON_STREAM, TOPICS[turn % len(TOPICS)], Partitioning.balanced(), ms)
        turn += 1
        sent += per_tick
        if time.monotonic() - mark >= 1:
            print(f"[beacons] {sent:,} beacons/s  ~{sent * a.size / 1e6:.1f} MB/s on {len(TOPICS)} SDK-family topics",
                  flush=True)
            sent, mark = 0, time.monotonic()
        tick = max(tick + 0.02, time.monotonic() - 0.1)
        await asyncio.sleep(max(0.0, tick - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=600, help="bytes per beacon")
    ap.add_argument("--envs", type=int, default=150)
    ap.add_argument("--partitions", type=int, default=16)
    asyncio.run(main(ap.parse_args()))
