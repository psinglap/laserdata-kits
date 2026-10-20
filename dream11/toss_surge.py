"""A busy neighbour on the same broker: team saves and contest joins, with the surge after the toss.

Team edits and contest joins on their own topic, at a steady rate and then --x times the rate
for --for seconds: "After the toss of every match, we typically see a high surge in contest
joins per second" (Dream11 Engineering blog, 2021). At the peak the defaults send 28,000 a
second: the "20000+ team updates per second" and "8000+ contest joins per second" Dream11
reported for IPL 2020. Run it while scoring.py and points_node.py run, and watch whether the
points lane's p99 moves when the surge hits. One batch per tick, spread over the partitions
by the broker.
"""
import argparse
import asyncio
import gc
import json
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, STREAM, SURGE, connect, ensure_topic, match_ids


def event(rng, n, matches):
    kind = "team.save" if rng.random() < 20 / 28 else "contest.join"
    e = {"event": kind, "user_id": rng.randrange(1, 200_000_000), "match": rng.choice(matches), "team_no": rng.randint(1, 6),
         "ts": time.time_ns(), "n": n}
    if kind == "team.save":
        e["players"] = rng.sample(range(22), 11)
        e["captain"], e["vice_captain"] = e["players"][0], e["players"][1]
    else:
        e["contest_id"] = f"C{rng.randrange(1, 50_000):05d}"
    return json.dumps(e, separators=(",", ":")).encode()


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, SURGE, 8)
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
            await c.send_messages(STREAM, SURGE, Partitioning.balanced(), ms)
            sent += k
        if time.monotonic() - last >= 1:
            tag = f"  toss surge {a.x:g}x" if x > 1 else ""
            print(f"[toss-surge] {sent:,} team saves and contest joins/s{tag}", flush=True)
            sent, last = 0, time.monotonic()
        due += 0.01
        await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=7000, help="events per second before the surge")
    p.add_argument("--x", type=float, default=4, help="surge multiplier (7,000 x 4 = 28,000 a second)")
    p.add_argument("--at", type=float, default=30, help="surge starts this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=20)
    asyncio.run(main(p.parse_args()))
