"""Shopping events from subscribers' sessions, keyed by subscriber, into the decision lane.

Plays the producers in Attentive's decision-engine post: services that emit user-intent events
(product views, add to cart, checkout started, purchase, clicks on a text or an email) for the
decision engine to act on. Each session is a short burst: a few views, often several cart
updates, sometimes a checkout and a purchase, 50 to 600 ms apart. That burst is the case the
post describes: two triggers from one subscriber, read by two consumers at once, can become two
texts.

Every event carries the subscriber ID, its brand, a per-subscriber sequence number and the time
it was written. With the default --route key, all of a subscriber's events go to the same
partition (crc32 of the subscriber ID). --route spread scatters them across partitions instead,
like a shared subscription with no key, to show what the decision workers then get wrong.
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS_TOPIC, INTENT, STREAM, brand_of, connect, ensure_topic, msg, part, subscriber_id


def session(rnd: random.Random) -> list:
    """One shopping session: event names in order."""
    out = ["sms_click"] if rnd.random() < 0.12 else (["email_click"] if rnd.random() < 0.08 else [])
    out += ["product_view"] * rnd.randint(1, 6)
    if rnd.random() < 0.55:
        for _ in range(rnd.choice((1, 1, 2, 2, 3))):            # cart edits: several triggers in a row
            out += ["add_to_cart"] + ["product_view"] * rnd.randint(0, 1)
        if rnd.random() < 0.35:
            out.append("checkout_started")
            if rnd.random() < 0.4:
                out.append("purchase")
    return out


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, EVENTS_TOPIC, a.partitions)
    parts = (await admin.get_topic(STREAM, EVENTS_TOPIC)).partitions_count
    writers = [await connect(a.url) for _ in range(a.writers)]
    rnd = random.Random(a.seed)
    seq = defaultdict(int)                           # subscriber -> last sequence written
    due = []                                         # (due time, tie, subscriber, event name, session id)
    stats = defaultdict(int)
    mean_len, owed, sid, tie = 5.0, 0.0, 0, 0
    peak_until = time.monotonic() + a.peak_s
    print(f"[storefront] writing to '{STREAM}/{EVENTS_TOPIC}' ({parts} partitions), route by "
          f"{'subscriber' if a.route == 'key' else 'nothing (spread)'}, {a.subscribers:,} subscribers", flush=True)
    gc.freeze()                                      # keep the collector from pausing the sender

    async def send(c, batch):
        await asyncio.gather(*(c.send_messages(STREAM, EVENTS_TOPIC, Partitioning.partition_id(pid), ms)
                               for pid, ms in batch.items()))

    async def run():
        nonlocal owed, sid, tie, mean_len
        tick = time.monotonic()
        while True:
            now = time.monotonic()
            rate = a.rate * (a.peak if now < peak_until else 1)
            owed += rate / mean_len * a.tick_ms / 1000
            for _ in range(int(owed)):               # start new sessions
                owed -= 1
                sid += 1
                sub = subscriber_id(rnd.randrange(a.subscribers))
                names = session(rnd)
                mean_len = 0.999 * mean_len + 0.001 * len(names)
                t = now
                for e in names:
                    tie += 1
                    heapq.heappush(due, (t, tie, sub, e, sid))
                    t += rnd.uniform(0.05, 0.6)
                stats["sessions"] += 1
                if sum(e in INTENT for e in names) >= 2:
                    stats["multi"] += 1              # two or more triggers in one burst
            batches = [defaultdict(list) for _ in writers]
            ns = time.time_ns()
            while due and due[0][0] <= now:
                _, _, sub, e, s = heapq.heappop(due)
                seq[sub] += 1
                pid = part(sub, parts) if a.route == "key" else rnd.randrange(parts)
                body = dict(ns=ns, u=sub, b=brand_of(sub, a.brands), e=e, seq=seq[sub], sess=s)
                if a.route != "key":
                    body["x"] = 1                    # spread: per-subscriber order is not kept
                batches[pid % len(writers)][pid].append(SendMessage(msg(**body)))
                stats["events"] += 1
                stats["intent"] += e in INTENT
            # One batch per partition per tick, each writer on its own connection.
            await asyncio.gather(*(send(c, b) for c, b in zip(writers, batches) if b))
            tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)   # never bank more than 100 ms
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            peak = " (peak)" if time.monotonic() < peak_until else ""
            print(f"[storefront] {stats['events']:>7,} shopping events/s{peak}  {stats['sessions']:>6,} sessions/s  "
                  f"{stats['intent']:>6,} triggers/s (add_to_cart, checkout_started)  "
                  f"{stats['multi']:>5,} sessions/s with 2+ triggers", flush=True)
            stats.clear()

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=30_000,
                   help="shopping events a second (10B events over BFCM weekend is ~29K/s on average)")
    p.add_argument("--peak", type=float, default=2.0, help="multiplier for the first --peak-s seconds (flash sale)")
    p.add_argument("--peak-s", type=float, default=60)
    p.add_argument("--subscribers", type=int, default=20_000_000, help="subscribers shopping in this window")
    p.add_argument("--brands", type=int, default=8_000)
    p.add_argument("--partitions", type=int, default=64, help="partitions for a new events topic")
    p.add_argument("--route", choices=("key", "spread"), default="key")
    p.add_argument("--writers", type=int, default=2, help="connections sending in parallel")
    p.add_argument("--tick-ms", type=float, default=20)
    p.add_argument("--seed", type=int, default=7)
    asyncio.run(main(p.parse_args()))
