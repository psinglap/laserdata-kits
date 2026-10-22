"""A heavy neighbour: full sensor packets on their own topic, with the burst a reconnect storm brings.

Ather's scooters send data from dozens of sensors at high frequency, and a network drop followed by sudden
reconnections has put heavy back-pressure on the pipeline. This script plays that stream: --mbps of packets
all the time, --burst-x times that for --burst-for seconds from --burst-at (the flushed sensor data of the
scooters coming back online). Use it to check that the bulk stream on the same broker does not move the rider
events lane's p99 (watch alert_service.py while this runs).

One batch per 10 ms tick, spread over partitions by the broker, sized to what the tick owes, so the neighbour
itself does not flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, STREAM, TELEMETRY, ensure_topics, msg, scooter


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, TELEMETRY)
    p = await client.producer(STREAM, TELEMETRY, topic_partitions_count=PARTITIONS[TELEMETRY],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for a packet of sensor readings
    if a.at:
        print(f"[telemetry] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    t_start = time.monotonic()
    due, sent, t0, end = t_start, 0, t_start, t_start + (a.duration or 1e12)
    owed, last = 0.0, t_start
    async with p:
        while time.monotonic() < end:
            now = time.monotonic()
            el = now - t_start
            burst = a.burst_x if a.burst_for and a.burst_at <= el < a.burst_at + a.burst_for else 1
            owed += a.mbps * burst * 1e6 / a.size * (now - last)   # a slow send is made up in the next batch
            last, n = now, int(owed)
            owed -= n
            if n:
                ms = [SendMessage(msg(s=scooter(random.randrange(1_000_000)), packet=blob)) for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                tag = " (reconnect burst)" if burst > 1 else ""
                print(f"[telemetry] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{TELEMETRY}'{tag}", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[telemetry] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second, steady")
    ap.add_argument("--size", type=int, default=4096, help="bytes per sensor packet")
    ap.add_argument("--burst-x", type=float, default=3, help="rate multiplier during the reconnect burst")
    ap.add_argument("--burst-at", type=float, default=0, help="seconds after this script starts sending")
    ap.add_argument("--burst-for", type=float, default=0, help="seconds (0 = no burst)")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
