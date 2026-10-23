"""A busy neighbour on the same broker: player viewing events, with a surge when the evening slate's first pitches go out.

Fubo's data team builds the pipelines behind product analytics, recommendations and personalization (Fubo Data
Engineering posting, 2026). This script writes viewing events (session, channel, device, position, bitrate) to
their own topic at a steady rate, then --x times the rate for --for seconds, as when games start at once and
viewers tune in together. Run it while moments.py and game_alerts.py run, and watch whether the alerts lane's p99
moves when the surge hits. One batch per tick, spread over the partitions by the broker.
"""
import argparse
import asyncio
import gc
import json
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, NEIGHBOUR, STREAM, connect, ensure_topic, game_ids

DEVICES = ["roku", "apple-tv", "fire-tv", "smart-tv", "ios", "android", "web"]
EVENTS = ["heartbeat"] * 8 + ["start", "pause", "seek", "bitrate-change"]


def viewing(rng, n, games):
    return json.dumps({"ev": rng.choice(EVENTS), "session": f"s{rng.randrange(1, 10**9):09d}",
                       "game": rng.choice(games), "device": rng.choice(DEVICES), "pos_s": rng.randrange(0, 12000),
                       "kbps": rng.choice([2500, 4500, 8000, 16000]), "ts": time.time_ns() // 10**6, "n": n},
                      separators=(",", ":")).encode()


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, NEIGHBOUR, 8)
    rng, n, games = random.Random(3), 0, game_ids(15)
    gc.freeze()
    t0 = time.monotonic()
    due, carry, sent, last = t0, 0.0, 0, t0
    while True:
        el = time.monotonic() - t0
        x = a.x if a.at <= el < a.at + a.for_s else 1
        carry += a.rate * x / 100
        k, carry = int(carry), carry - int(carry)
        if k:
            ms = []
            for _ in range(k):
                n += 1
                ms.append(SendMessage(viewing(rng, n, games)))
            await c.send_messages(STREAM, NEIGHBOUR, Partitioning.balanced(), ms)
            sent += k
        if time.monotonic() - last >= 1:
            tag = f"  first-pitch surge {a.x:g}x" if x > 1 else ""
            print(f"[viewing] {sent:,} viewing events/s{tag}", flush=True)
            sent, last = 0, time.monotonic()
        due += 0.01                                      # from the due time, so a slow tick doesn't drift
        await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=5000, help="viewing events per second before the surge")
    p.add_argument("--x", type=float, default=4, help="surge multiplier")
    p.add_argument("--at", type=float, default=30, help="surge starts this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=20)
    asyncio.run(main(p.parse_args()))
