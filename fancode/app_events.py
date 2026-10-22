"""A busy neighbour on the same broker: app events, with a surge when a big match starts.

FanCode's web app batches analytics events (event name plus props: session, page, screen, user agent) and
posts them to its data highway every 3 s or every 10 events. This script writes events of that shape to
their own topic, at a steady rate and then --x times the rate for --for seconds, as when a headline match
goes live and viewers pour in. Run it while score_feed.py and scorecard_node.py run, and watch whether the
scorecard lane's p99 moves when the surge hits. One batch per tick, spread over the partitions by the broker.
"""
import argparse
import asyncio
import gc
import json
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, NEIGHBOUR, STREAM, connect, ensure_topic, match_ids

EVENTS = ["page_view", "match_card_click", "video_play", "HeartBeat", "scorecard_tab_click", "commentary_scroll"]


def event(rng, n, matches):
    return json.dumps({"eventName": rng.choice(EVENTS),
                       "props": {"match_id": rng.choice(matches), "dh_session_id": f"s{rng.randrange(1, 10**9):09d}",
                                 "app_name": "fc-web", "screen_resolution": "1920x1080", "eventTime": time.time_ns() // 10**6,
                                 "current_page_url": "https://www.fancode.com/cricket/live", "n": n}},
                      separators=(",", ":")).encode()


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, NEIGHBOUR, 8)
    rng, n, matches = random.Random(1), 0, match_ids(40)
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
                ms.append(SendMessage(event(rng, n, matches)))
            await c.send_messages(STREAM, NEIGHBOUR, Partitioning.balanced(), ms)
            sent += k
        if time.monotonic() - last >= 1:
            tag = f"  match-start surge {a.x:g}x" if x > 1 else ""
            print(f"[app-events] {sent:,} app events/s{tag}", flush=True)
            sent, last = 0, time.monotonic()
        due += 0.01                                         # from the due time, so a slow tick doesn't drift
        await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=5000, help="events per second before the surge")
    p.add_argument("--x", type=float, default=4, help="surge multiplier")
    p.add_argument("--at", type=float, default=30, help="surge starts this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=20)
    asyncio.run(main(p.parse_args()))
