"""A heavy neighbour: app and marketing events on their own topic, the shape of a high-volume, latency-tolerant
stream on the same broker (clickstream and campaign events that land in the data lake, not in a matchmaker).

Use it to check that a bulk stream next door does not move the player form lane's p99 (watch matchmaker.py while
this runs). One batch per 10 ms tick, spread over partitions by the broker, sized to what the tick owes, so the
neighbour itself does not flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import APP_EVENTS, DEFAULT_URL, STREAM, connect, ensure_topics, msg


async def main(a):
    client = await connect(a.url)
    await ensure_topics(client, APP_EVENTS)
    blob = os.urandom(a.size // 2).hex()       # stands in for the event's fields
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[app-events] starting in {a.at:g} s", flush=True)
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
            ms = [SendMessage(msg(event=random.choice(("app_open", "lobby_view", "banner_click", "campaign_install")),
                                  user=f"P{random.randrange(10**8):08d}", body=blob))
                  for _ in range(n)]
            await client.send_messages(STREAM, APP_EVENTS, Partitioning.balanced(), ms)
            sent += n
        if time.monotonic() - t0 >= 1:
            print(f"[app-events] {sent:,} events/s  ~{sent * a.size / 1e6:.1f} MB/s on '{APP_EVENTS}'", flush=True)
            sent, t0 = 0, time.monotonic()
        due += 0.01
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[app-events] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1024, help="bytes per event")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
