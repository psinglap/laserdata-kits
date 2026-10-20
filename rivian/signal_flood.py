"""A heavy neighbour: the full signal set of every vehicle report, on its own topic on the same broker.

Each record stands in for one report's thousands of signals: big, steady and read by analytics, not by the
app. Use it to check that this volume next door does not move the vehicle state lane's p99 (watch
vehicle_state.py while this runs). The latest-state service never reads this topic, so it never has to skip
past signals it does not need; that is the design rule this test holds the broker to.

One batch per 10 ms tick, spread over partitions by the broker, sized to what the tick owes, so the
neighbour itself does not flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, SIGNALS, STREAM, ensure_topics, msg, vehicle


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, SIGNALS)
    p = await client.producer(STREAM, SIGNALS, topic_partitions_count=PARTITIONS[SIGNALS],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for one report's packed signal values
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[signals] starting in {a.at:g} s", flush=True)
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
                ms = [SendMessage(msg(v=vehicle(random.randrange(1_000_000)), signals=a.signals, packed=blob))
                      for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[signals] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{SIGNALS}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[signals] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=100, help="megabytes per second")
    ap.add_argument("--size", type=int, default=16384, help="bytes per full report record")
    ap.add_argument("--signals", type=int, default=5500, help="signals per report (a label on each record)")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
