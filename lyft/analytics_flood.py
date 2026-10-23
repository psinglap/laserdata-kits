"""A heavy neighbour: analytics events (app opens, ride requests, driver status, location-derived signals) on their
own topic, the shape of the event stream that feeds feature generation, on the same broker as the forecast lane.

Lyft's streaming post describes the feature generation pipeline ingesting tens of millions of events per second,
and steady throughput through peaks like New Year's Eve and Halloween. Use this to check that a bulk stream next
door does not move the forecast lane's p99 (watch marketplace.py while it runs). One batch per 10 ms tick, spread
over partitions by the broker, sized to what the tick owes, so the neighbour does not flood the broker with tiny
requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import ANALYTICS, DEFAULT_URL, STREAM, connect, ensure_topics, msg

EVENTS = ("app_open", "ride_request", "price_view", "driver_online", "driver_offline", "trip_state")


async def main(a):
    client = await connect(a.url)
    await ensure_topics(client, ANALYTICS)
    blob = os.urandom(a.size // 2).hex()       # stands in for the event's fields
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[analytics] starting in {a.at:g} s", flush=True)
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
            ms = [SendMessage(msg(event=random.choice(EVENTS), user=f"U{random.randrange(10**9):09d}", body=blob))
                  for _ in range(n)]
            await client.send_messages(STREAM, ANALYTICS, Partitioning.balanced(), ms)
            sent += n
        if time.monotonic() - t0 >= 1:
            print(f"[analytics] {sent:,} events/s  ~{sent * a.size / 1e6:.1f} MB/s on '{ANALYTICS}'", flush=True)
            sent, t0 = 0, time.monotonic()
        due += 0.01
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[analytics] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1024, help="bytes per event")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
