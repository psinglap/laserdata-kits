"""Pocket FM listeners: they start shows, finish episodes, unlock the next one, skip Autoplay picks and finish shows.

Plays the app. Each listening action is sent, keyed by user, to:
  * listening-actions: the lane. The action (play, episode_complete, unlock with coins, an ad or a
    subscription, skip, show_complete), the show, the listener's session and a sequence number that only
    grows for each user, for the Autoplay queue service;
  * autoplay-requests: a short time after an action, the player reaches the end of content (an episode
    ends with no unlocked episode after it, a show ends, or a pick is skipped) and asks for the next show.
    The request carries the sequence of the listener's last action, which the queue must already hold,
    and the show Autoplay should start after it.

Some plays start the show at the head of the Autoplay queue (--autoplay-share), the rest a show picked
elsewhere in the app; skips drop the head of the queue. --gap-min/--gap-max set the time from an action
to the next end-of-content request; --surge multiplies the rate for --surge-for seconds (as when the daily
episodes of a hit show drop and its listeners all come back at once).
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (ACTIONS, AUTOPLAY, CATALOG, DEFAULT_URL, KINDS, PARTITIONS, STREAM, UNLOCKS, ensure_topics,
                    fold, head, msg, part, show_id, user_id)

KIND_MIX = [0.30, 0.35, 0.15, 0.12, 0.08]       # play, episode_complete, unlock, skip, show_complete (illustrative)


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ACTIONS, AUTOPLAY)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    act_p = await (await own_client()).producer(STREAM, ACTIONS, topic_partitions_count=PARTITIONS[ACTIONS], mode=cfg)
    req_p = await (await own_client()).producer(STREAM, AUTOPLAY, topic_partitions_count=PARTITIONS[AUTOPLAY], mode=cfg)
    # Action sequence: one counter for all users, started from the clock, so each user's sequence only grows,
    # even across restarts of this script, without keeping a counter per user.
    next_seq = iter(range(time.time_ns(), 1 << 63))
    run = os.urandom(3).hex()               # sessions from this run of the app
    listeners = {}                          # user -> [session, queue state]
    pending = []                            # (when, user, needs, next show, moved): the next end-of-content request
    stats = defaultdict(int)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def action(user, now_ns):
        s = listeners.get(user)
        if s is None:
            s = listeners[user] = [f"{run}-{user}", None]
        q = s[1]
        nxt = head(q or fold(None, user, s[0], "episode_complete", ""))  # what Autoplay would start now
        kind = random.choices(KINDS, KIND_MIX)[0]
        playing = q[1] if q else ""
        extra = {}
        if kind == "play":
            if random.random() < a.autoplay_share:
                show = nxt or show_id(random.randrange(CATALOG))
                stats["from_queue"] += 1    # the listener let Autoplay start its pick
            else:
                show = show_id(random.randrange(CATALOG))
        elif kind == "skip":
            show = nxt                      # the Autoplay pick, skipped
            if not show:                    # nothing offered yet (or the queue is used up): a play instead
                kind, show = "play", show_id(random.randrange(CATALOG))
        else:
            show = playing or show_id(random.randrange(CATALOG))
            if kind == "unlock":
                extra["via"] = random.choice(UNLOCKS)
        before = head(s[1] or fold(None, user, s[0], "episode_complete", ""))   # the next show before this action
        s[1] = fold(s[1], user, s[0], kind, show)   # what the queue should hold after this action
        after = head(s[1])
        seq = next(next_seq)
        stats[kind] += 1
        if random.random() < a.request_share:
            heapq.heappush(pending, (time.monotonic() + random.uniform(a.gap_min, a.gap_max), user, seq, after,
                                     before != after))
        return SendMessage(msg(user=user, sess=s[0], seq=seq, kind=kind, show=show, ns=now_ns, **extra))

    def request(user, needs, nxt, moved, now_ns):
        stats["requests"] += 1
        return SendMessage(msg(user=user, needs=needs, next=nxt, moved=moved, ns=now_ns))

    async def send(producer, items, parts):
        # One batch per partition, all sent at once: per-user order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for user, m in items:
            groups[part(user, parts)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def run_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        async with act_p, req_p:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.surge if a.surge_at <= t < a.surge_at + a.surge_for else 1
                owed += a.actions_per_s * x / 100
                now = time.time_ns()
                acts = []
                for _ in range(int(owed)):
                    u = user_id(random.randrange(a.users))
                    acts.append((u, action(u, now)))
                owed -= int(owed)
                reqs = []
                while pending and pending[0][0] <= now_s:
                    _, u, needs, nxt, moved = heapq.heappop(pending)
                    reqs.append((u, request(u, needs, nxt, moved, time.time_ns())))
                await asyncio.gather(send(act_p, acts, PARTITIONS[ACTIONS]), send(req_p, reqs, PARTITIONS[AUTOPLAY]))
                stats["surge"] = x > 1
                due += 0.01                 # from the due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            # Move what survived the last second (listeners, queues) out of the collector's reach, so a full
            # collection never walks hundreds of thousands of long-lived objects and pauses the producers.
            gc.freeze()
            phase = "surge" if stats["surge"] else "steady"
            n = sum(stats[k] for k in KINDS)
            print(f"[listeners] {n:>7,} listening actions/s ({phase})  play {stats['play']:,} "
                  f"(Autoplay picks {stats['from_queue']:,})  episode complete {stats['episode_complete']:,}  "
                  f"unlock {stats['unlock']:,}  skip {stats['skip']:,}  show complete {stats['show_complete']:,}  "
                  f"end-of-content requests {stats['requests']:,}  listeners in session {len(listeners):,}", flush=True)
            for k in KINDS + ["from_queue", "requests"]:
                stats[k] = 0

    await asyncio.gather(run_loop(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--users", type=int, default=1_000_000, help="listeners in session (illustrative)")
    p.add_argument("--actions-per-s", type=float, default=20_000, help="plays, completes, unlocks, skips a second")
    p.add_argument("--autoplay-share", type=float, default=0.6, help="share of plays that start the Autoplay pick")
    p.add_argument("--request-share", type=float, default=0.5, help="share of actions followed by an end-of-content request")
    p.add_argument("--gap-min", type=float, default=0.3, help="seconds from an action to the next end-of-content request, at least")
    p.add_argument("--gap-max", type=float, default=2.0, help="seconds from an action to the next end-of-content request, at most")
    p.add_argument("--surge", type=float, default=2.0, help="rate multiplier when a hit's daily episodes drop")
    p.add_argument("--surge-at", type=float, default=0, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
