"""A heavy neighbour: one customer's error storm, on its own topic on the same broker.

A bad deploy somewhere sends a flood of error events. Each record here stands in for one error event: a few
KB, steady, and read by the event pipeline, not by the monitor consumer. Use it to check that this volume
next door does not move the check-in lane's p99 or the clock's tick lag (watch monitors.py while this runs).
The monitor consumer never reads this topic, so it never has to skip past event bodies it does not need;
that is the design rule this test holds the broker to.

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

from common import DEFAULT_URL, EVENTS, PARTITIONS, STREAM, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, EVENTS)
    p = await client.producer(STREAM, EVENTS, topic_partitions_count=PARTITIONS[EVENTS],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    frames = os.urandom(a.size // 2).hex()     # stands in for the stack trace and context of one event
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[storm] starting in {a.at:g} s", flush=True)
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
                ms = [SendMessage(msg(project_id=a.project, level="error", fingerprint=random.randrange(50),
                                      exception=frames)) for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[storm] {sent:,} events/s  ~{sent * a.size / 1e6:.1f} MB/s on '{EVENTS}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[storm] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=100, help="megabytes per second")
    ap.add_argument("--size", type=int, default=4096, help="bytes per error event")
    ap.add_argument("--project", type=int, default=4242, help="the project sending the storm (a label)")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
