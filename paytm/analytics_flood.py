"""A heavy neighbour: app and behavioural events for analytics on their own topic, with a burst.

Paytm has written that its platform generates more than 5 billion events a day for real-time behavioural
analytics (Imply case study, 2022). This script plays that kind of stream on the same broker as the payment
confirmations lane: --mbps of events all the time, --burst-x times that for --burst-for seconds from --burst-at
(a campaign or a festival-evening spike). Use it to check that the bulk stream does not move the lane's p99
(watch announcer.py while this runs).

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

from common import APP_EVENTS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, APP_EVENTS)
    p = await client.producer(STREAM, APP_EVENTS, topic_partitions_count=PARTITIONS[APP_EVENTS],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()       # stands in for an event's properties
    if a.at:
        print(f"[app-events] starting in {a.at:g} s", flush=True)
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
                ms = [SendMessage(msg(u=f"u{random.randrange(80_000_000)}", ev="screen_view", props=blob))
                      for _ in range(n)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                tag = " (burst)" if burst > 1 else ""
                print(f"[app-events] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{APP_EVENTS}'{tag}", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[app-events] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second, steady")
    ap.add_argument("--size", type=int, default=1024, help="bytes per event")
    ap.add_argument("--burst-x", type=float, default=3, help="rate multiplier during the burst")
    ap.add_argument("--burst-at", type=float, default=0, help="seconds after this script starts sending")
    ap.add_argument("--burst-for", type=float, default=0, help="seconds (0 = no burst)")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
