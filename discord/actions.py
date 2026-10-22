"""Osprey actions from Discord's services, keyed by user, into the action lane.

Plays the services that send actions to Osprey asynchronously, through a message queue: logins,
messages sent, identity changes and guild joins. Most sessions are people: an occasional login, a few
messages seconds apart, rarely a link. A share of sessions (--spam-share) is a fresh account doing what
a spam bot does: a login, then a run of messages with links 80 to 250 ms apart. That run is the case the
lane is for: the rule that labels the account fires on one message, and the next message, a fraction of
a second later, must already be evaluated with the label in place.

Every action carries the user ID, the action name, the account's age, whether the message has a link,
a per-user sequence number and the time it was written. With the default --route key, all of a user's
actions go to the same partition (crc32 of the user ID). --route spread scatters them across
partitions instead, like a queue with no ordering key, to show what the rules workers then miss.
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import ACTIONS_TOPIC, DEFAULT_URL, STREAM, connect, ensure_topic, msg, part, user_id


def person(rnd: random.Random) -> list:
    """One person's session: (action, has link, gap to the next action in seconds)."""
    out = [("user_login_attempted", 0, rnd.uniform(0.5, 2.0))] if rnd.random() < 0.25 else []
    if rnd.random() < 0.08:
        out.append(("guild_join", 0, rnd.uniform(1.0, 3.0)))
    for _ in range(rnd.randint(1, 6)):
        out.append(("send_message", int(rnd.random() < 0.06), rnd.uniform(0.3, 4.0)))
    if rnd.random() < 0.02:
        out.append(("identity_change", 0, 0.0))
    return out


def bot(rnd: random.Random) -> list:
    """A fresh account's spam run: a login, then messages with links in quick succession."""
    out = [("user_login_attempted", 0, rnd.uniform(0.2, 0.8))]
    out += [("send_message", 1, rnd.uniform(0.08, 0.25)) for _ in range(rnd.randint(8, 20))]
    return out


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, ACTIONS_TOPIC, a.partitions)
    parts = (await admin.get_topic(STREAM, ACTIONS_TOPIC)).partitions_count
    writers = [await connect(a.url) for _ in range(a.writers)]
    rnd = random.Random(a.seed)
    seq = defaultdict(int)                           # user -> last sequence written
    due = []                                         # (due time, tie, user, action, link, age in hours, session)
    stats = defaultdict(int)
    mean_len, owed, sid, tie, fresh = 4.0, 0.0, 0, 0, 0
    peak_until = time.monotonic() + a.peak_s
    print(f"[actions] writing to '{STREAM}/{ACTIONS_TOPIC}' ({parts} partitions), route by "
          f"{'user' if a.route == 'key' else 'nothing (spread)'}, {a.users:,} active users, "
          f"{a.spam_share:.0%} of sessions are spam runs", flush=True)
    gc.freeze()                                      # keep the collector from pausing the sender

    async def send(c, batch):
        await asyncio.gather(*(c.send_messages(STREAM, ACTIONS_TOPIC, Partitioning.partition_id(pid), ms)
                               for pid, ms in batch.items()))

    async def run():
        nonlocal owed, sid, tie, mean_len, fresh
        tick = time.monotonic()
        while True:
            now = time.monotonic()
            rate = a.rate * (a.peak if now < peak_until else 1)
            owed += rate / mean_len * a.tick_ms / 1000
            for _ in range(int(owed)):               # start new sessions
                owed -= 1
                sid += 1
                if rnd.random() < a.spam_share:
                    fresh += 1
                    u, age, steps = user_id(a.users + fresh), rnd.uniform(0.1, 12), bot(rnd)
                    stats["runs"] += 1
                else:
                    u, age, steps = user_id(rnd.randrange(a.users)), rnd.uniform(24, 24 * 2000), person(rnd)
                mean_len = 0.999 * mean_len + 0.001 * len(steps)
                t = now
                for name, link, gap in steps:
                    tie += 1
                    heapq.heappush(due, (t, tie, u, name, link, age, sid))
                    t += gap
                stats["sessions"] += 1
            batches = [defaultdict(list) for _ in writers]
            ns = time.time_ns()
            while due and due[0][0] <= now:
                _, _, u, name, link, age, s = heapq.heappop(due)
                seq[u] += 1
                pid = part(u, parts) if a.route == "key" else rnd.randrange(parts)
                body = dict(ns=ns, u=u, a=name, seq=seq[u], age_h=round(age, 1), link=link, sess=s)
                if a.route != "key":
                    body["x"] = 1                    # spread: per-user order is not kept
                batches[pid % len(writers)][pid].append(SendMessage(msg(**body)))
                stats["actions"] += 1
                stats["messages"] += name == "send_message"
            # One batch per partition per tick, each writer on its own connection.
            await asyncio.gather(*(send(c, b) for c, b in zip(writers, batches) if b))
            tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)   # never bank more than 100 ms
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            peak = " (raid peak)" if time.monotonic() < peak_until else ""
            print(f"[actions] {stats['actions']:>7,} actions/s{peak}  {stats['messages']:>7,} send_message/s  "
                  f"{stats['sessions']:>6,} sessions/s  {stats['runs']:>4,} spam runs/s", flush=True)
            stats.clear()

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=10_000,
                   help="actions a second (~400 million a day is ~4,600/s on average; this is about 2x that)")
    p.add_argument("--peak", type=float, default=3.0, help="multiplier for the first --peak-s seconds (a raid)")
    p.add_argument("--peak-s", type=float, default=60)
    p.add_argument("--users", type=int, default=5_000_000, help="active users sending actions in this window")
    p.add_argument("--spam-share", type=float, default=0.03, help="share of sessions that are spam runs")
    p.add_argument("--partitions", type=int, default=64, help="partitions for a new actions topic")
    p.add_argument("--route", choices=("key", "spread"), default="key")
    p.add_argument("--writers", type=int, default=2, help="connections sending in parallel")
    p.add_argument("--tick-ms", type=float, default=20)
    p.add_argument("--seed", type=int, default=7)
    asyncio.run(main(p.parse_args()))
