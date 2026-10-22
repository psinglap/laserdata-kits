"""Tracked events from users' apps and sites, keyed by user, into the trip lane.

Plays the SDKs and the /events REST API: every event carries the user ID, the event name, the
context attribute's value (the Policy ID, Loan ID or Category ID of the trip it belongs to), a
per-user sequence number and the time it was written.

Each new trip is one user entering one Contextual Journey for one context:
  * the entry event (Renewal Due, Loan Application Started, Category Viewed);
  * a few activity events for the same context while the trip waits;
  * for --convert-before of trips, the exit event (Policy Renewed, Payment Successful, Added To Cart)
    shortly before the reminder is due: --gap-min-s to --gap-max-s ahead of it. That is the race the
    lane has to win: the exit must reach the trip before its reminder goes out;
  * for --convert-after of trips, the exit after the reminder (the reminder worked);
  * the rest never convert, and get their reminder.
--parallel of new trips go to a user who already has a trip running, so users hold several trips of
the same journey at once, one per context, as Contextual Journeys allow.

The wait before the reminder is compressed (--wait-s, seconds instead of hours or days) so a run of a
minute covers thousands of complete trips.
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict, deque

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS_TOPIC, JOURNEYS, STREAM, connect, ensure_topic, msg, part, user_id

PREFIX = {"policy_id": "POL", "loan_id": "LN", "category_id": "CAT"}


def plan(rnd: random.Random, a, journey: str, t0: float):
    """One trip's events as (time, event name)."""
    d = JOURNEYS[journey]
    out = [(t0, d["entry"])]
    for _ in range(rnd.randint(0, 3)):
        out.append((t0 + rnd.uniform(0.05, a.wait_s * 1.4), rnd.choice(d["activity"])))
    r = rnd.random()
    if r < a.convert_before:
        out.append((t0 + a.wait_s - rnd.uniform(a.gap_min_s, a.gap_max_s), d["exit"]))
    elif r < a.convert_before + a.convert_after:
        out.append((t0 + a.wait_s + rnd.uniform(0.2, 1.0), d["exit"]))
    return out


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, EVENTS_TOPIC, a.partitions)
    parts = (await admin.get_topic(STREAM, EVENTS_TOPIC)).partitions_count
    writers = [await connect(a.url) for _ in range(a.writers)]
    rnd = random.Random(a.seed)
    names = list(JOURNEYS)
    weights = [JOURNEYS[j]["share"] for j in names]
    seq = defaultdict(int)                           # user -> last sequence written
    due = []                                         # (due time, tie, user, event name, context value)
    recent = deque(maxlen=5000)                      # users with a trip started lately
    stats = defaultdict(int)
    mean_len, owed, tie, ctx_n = 3.0, 0.0, 0, 0
    peak_until = time.monotonic() + a.peak_s
    print(f"[sdk] writing to '{STREAM}/{EVENTS_TOPIC}' ({parts} partitions), keyed by user, {a.users:,} users, "
          f"reminder after {a.wait_s:g} s (compressed), exits {a.gap_min_s:g}-{a.gap_max_s:g} s before it", flush=True)
    gc.freeze()                                      # keep the collector from pausing the sender

    async def send(c, batch):
        await asyncio.gather(*(c.send_messages(STREAM, EVENTS_TOPIC, Partitioning.partition_id(pid), ms)
                               for pid, ms in batch.items()))

    async def run():
        nonlocal owed, tie, mean_len, ctx_n
        tick = time.monotonic()
        while True:
            now = time.monotonic()
            rate = a.rate * (a.peak if now < peak_until else 1)
            owed += rate / mean_len * a.tick_ms / 1000
            for _ in range(int(owed)):               # start new trips
                owed -= 1
                if recent and rnd.random() < a.parallel:
                    u = rnd.choice(recent)           # a second (or third) trip for a user already in one
                    stats["parallel"] += 1
                else:
                    u = user_id(rnd.randrange(a.users))
                recent.append(u)
                j = rnd.choices(names, weights)[0]
                ctx_n += 1
                c = f"{PREFIX[JOURNEYS[j]['ctx']]}-{a.seed}-{ctx_n}"   # unique context value per trip
                evs = plan(rnd, a, j, now)
                mean_len = 0.999 * mean_len + 0.001 * len(evs)
                for t, e in evs:
                    tie += 1
                    heapq.heappush(due, (t, tie, u, e, c))
                stats["trips"] += 1
            batches = [defaultdict(list) for _ in writers]
            ns = time.time_ns()
            while due and due[0][0] <= now:
                _, _, u, e, c = heapq.heappop(due)
                seq[u] += 1
                pid = part(u, parts)
                batches[pid % len(writers)][pid].append(SendMessage(msg(ns=ns, u=u, e=e, c=c, seq=seq[u])))
                stats["events"] += 1
            # One batch per partition per tick, each writer on its own connection.
            await asyncio.gather(*(send(c, b) for c, b in zip(writers, batches) if b))
            tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)   # never bank more than 100 ms
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            peak = " (peak)" if time.monotonic() < peak_until else ""
            print(f"[sdk] {stats['events']:>7,} tracked events/s{peak}  {stats['trips']:>6,} trips started/s  "
                  f"{stats['parallel']:>5,} of them a user's 2nd+ trip", flush=True)
            stats.clear()

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=50_000,
                   help="tracked events a second (1.6 trillion events a year is about 50K/s on average)")
    p.add_argument("--peak", type=float, default=2.0, help="multiplier for the first --peak-s seconds (sale or renewal day)")
    p.add_argument("--peak-s", type=float, default=60)
    p.add_argument("--users", type=int, default=20_000_000, help="users active in this window")
    p.add_argument("--partitions", type=int, default=64, help="partitions for a new events topic")
    p.add_argument("--wait-s", type=float, default=2.0, help="entry to reminder (hours in production; compressed)")
    p.add_argument("--convert-before", type=float, default=0.35, help="share of trips that exit just before the reminder")
    p.add_argument("--convert-after", type=float, default=0.15, help="share of trips that exit after the reminder")
    p.add_argument("--gap-min-s", type=float, default=0.1, help="closest an exit comes before the reminder is due")
    p.add_argument("--gap-max-s", type=float, default=0.8)
    p.add_argument("--parallel", type=float, default=0.3, help="share of new trips that go to a user already in a trip")
    p.add_argument("--writers", type=int, default=2, help="connections sending in parallel")
    p.add_argument("--tick-ms", type=float, default=20)
    p.add_argument("--seed", type=int, default=7)
    asyncio.run(main(p.parse_args()))
