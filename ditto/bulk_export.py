"""A heavy neighbour: a bulk export of historical documents to an analytics store on its own topic, the shape of
a high-volume, latency-tolerant stream on the same broker.

Use it to check that a bulk load next door does not move the CDC lane's p99 (watch mirror.py while this runs).
One batch per 10 ms tick, spread over partitions by the broker, sized to what the tick owes, so the neighbour
itself does not flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, EXPORT, STREAM, connect, dump, ensure_topics


async def main(a):
    client = await connect(a.url)
    await ensure_topics(client, EXPORT)
    blob = os.urandom(a.size // 2).hex()       # stands in for the archived document and its history
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[export] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    due, sent, t0, end = time.monotonic(), 0, time.monotonic(), time.monotonic() + (a.duration or 1e12)
    owed, last = 0.0, time.monotonic()
    while time.monotonic() < end:
        now = time.monotonic()
        owed += per_s * (now - last)        # what this tick owes, so a slow send is made up in the next batch
        last, n = now, int(owed)
        owed -= n
        if n:
            ms = [SendMessage(dump({"_id": f"FL{random.randrange(10**5):05d}-{random.randrange(1, 7)}A",
                                    "kind": "archived_order", "body": blob, "ns": time.time_ns()}))
                  for _ in range(n)]
            await client.send_messages(STREAM, EXPORT, Partitioning.balanced(), ms)
            sent += n
        if time.monotonic() - t0 >= 1:
            print(f"[export] {sent:,} records/s  ~{sent * a.size / 1e6:.1f} MB/s on '{EXPORT}'", flush=True)
            sent, t0 = 0, time.monotonic()
        due += 0.01
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[export] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=2048, help="bytes per record")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
