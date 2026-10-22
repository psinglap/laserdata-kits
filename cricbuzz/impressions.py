"""A busy neighbour on the same broker: ad impressions, with a surge at the innings break.

Cricbuzz served 110 billion impressions during IPL 2025 (Cricbuzz, November 2025). This script writes impression
events (placement, match, page, device) to their own topic, at a steady rate and then --x times the rate for
--for seconds, as when an innings ends and readers move from the commentary to the scorecard, the stats and the
videos. Run it while commentary_desk.py and the readers run, and watch whether the commentary lane's p99 moves
when the surge hits. One batch per tick, spread over the partitions by the broker.
"""
import argparse
import asyncio
import gc
import json
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, NEIGHBOUR, STREAM, connect, ensure_topic, match_ids

PAGES = ["live-commentary", "live-scorecard", "match-feed", "videos", "news", "series-stats"]
SLOTS = ["top-banner", "in-feed", "between-overs", "video-preroll"]


def impression(rng, n, matches):
    return json.dumps({"slot": rng.choice(SLOTS), "match": rng.choice(matches), "page": rng.choice(PAGES),
                       "device": rng.choice(["android", "ios", "web"]), "uid": f"u{rng.randrange(1, 10**9):09d}",
                       "ts": time.time_ns() // 10**6, "n": n}, separators=(",", ":")).encode()


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, NEIGHBOUR, 8)
    rng, n, matches = random.Random(1), 0, match_ids(30)
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
                ms.append(SendMessage(impression(rng, n, matches)))
            await c.send_messages(STREAM, NEIGHBOUR, Partitioning.balanced(), ms)
            sent += k
        if time.monotonic() - last >= 1:
            tag = f"  innings-break surge {a.x:g}x" if x > 1 else ""
            print(f"[impressions] {sent:,} impressions/s{tag}", flush=True)
            sent, last = 0, time.monotonic()
        due += 0.01                                      # from the due time, so a slow tick doesn't drift
        await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=5000, help="impressions per second before the surge")
    p.add_argument("--x", type=float, default=4, help="surge multiplier")
    p.add_argument("--at", type=float, default=30, help="surge starts this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=20)
    asyncio.run(main(p.parse_args()))
