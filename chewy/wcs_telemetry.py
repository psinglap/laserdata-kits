"""A busy neighbour on the building's broker: conveyor, sorter and scanner telemetry from the control system.

Chewy's controls engineers support "conveyors, sortation, scanners, cameras, print-and-apply, and robotics" in
its fulfillment centers. Their telemetry (photo-eye counts, jam and fault codes, motor and VFD readings, scanner
read rates) is high-volume and bursty, and nobody waits on any single reading.

Use it to check that a heavy, latency-tolerant stream on the same small broker does not move the carton lane's
p99 (watch site_state.py while this runs). One batch per 10 ms tick, spread over partitions by the broker, sized
to what the tick owes, so the neighbour itself does not flood the broker with tiny requests. Keeping telemetry on
its own topic is the design rule: a burst of sorter readings never queues ahead of a carton scan.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Partitioning, SendMessage

from common import DEFAULT_URL, FC, PARTITIONS, WCS, connect, ensure_topics, msg

DEVICES = ["conveyor-pe", "sorter-divert", "scanner-tunnel", "print-and-apply", "vfd", "robot-cell", "camera"]


async def main(a):
    client = await connect(a.url)
    await ensure_topics(client, WCS)
    p = await client.producer(FC, WCS, topic_partitions_count=PARTITIONS[WCS],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for a reading's tag values
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[wcs] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    due, sent, t0, end = time.monotonic(), 0, time.monotonic(), time.monotonic() + (a.duration or 1e12)
    owed, last = 0.0, time.monotonic()
    async with p:
        while time.monotonic() < end:
            now = time.monotonic()
            owed += per_s * (now - last)        # what this tick owes, so a slow send is made up in the next batch
            last, n = now, int(owed)
            owed -= n
            if n:
                ms = [SendMessage(msg(dev=random.choice(DEVICES), zone=random.randint(1, 40), tags=blob,
                                      ns=time.time_ns())) for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[wcs] {sent:,} readings/s  ~{sent * a.size / 1e6:.1f} MB/s on '{WCS}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[wcs] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL, help="the building's broker")
    ap.add_argument("--mbps", type=float, default=5, help="megabytes per second")
    ap.add_argument("--size", type=int, default=400, help="bytes per reading")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
