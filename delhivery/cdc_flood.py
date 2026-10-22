"""A heavy neighbour: change data capture from the transactional databases into the data lake, on its own topic.
Delhivery's engineers have described Debezium publishing database changes to Kafka for near-real-time analysis,
and a data lake fed by 800+ pipelines ingesting 60k messages per second through CDC connectors and Amazon MSK.
Those changes are bulky and bursty (a connector that re-snapshots a large table sends hours of rows at once) and
not urgent.

Use it to check that a high-volume, latency-tolerant stream on the same broker does not move the dock scan lane's
p99 (watch dock_check.py while this runs). One batch per 10 ms tick, spread over partitions by the broker, sized to
what the tick owes, so the neighbour itself does not flood the broker with tiny requests. Keeping CDC on its own
topic is the design rule: a re-snapshot never queues ahead of a load scan.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import CDC, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg

TABLES = ["shipments", "shipment_scans", "bags", "trips", "manifests", "consignees", "invoices", "pickups"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, CDC)
    p = await client.producer(STREAM, CDC, topic_partitions_count=PARTITIONS[CDC],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    row = os.urandom(a.size // 2).hex()        # stands in for a row image: before and after, as a connector sends it
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[cdc] starting in {a.at:g} s", flush=True)
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
                ms = [SendMessage(msg(table=random.choice(TABLES), op=random.choice("cuu"), lsn=random.getrandbits(40),
                                      row=row)) for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[cdc] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{CDC}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[cdc] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=2048, help="bytes per change record")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
