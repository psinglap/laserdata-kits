"""The apps: viewers start, switch and stop live events, then go back to the Live rail.

Plays the Disney+ and ESPN apps during a live sports window. Each time a profile starts a live event,
switches to another one or stops, the app sends a watch signal, keyed by profile, to:
  * watch-signals: the lane. The resulting state (what the profile is watching now, and what it just
    left), with a sequence number that only grows for each profile, for the live personalization
    feature service;
  * live-rail-requests: a short time after a signal, the profile's next Live rail request (back to
    home, or the Live shelf in the player). It carries the sequence of the profile's last signal,
    which the features must already hold, and the live events airing now, the rail's candidates.

--gap-min/--gap-max set the time from a signal to the next rail request; --surge multiplies the rate
for --surge-for seconds (a slate of games tipping off at once, when everyone switches).
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, RAILS, STREAM, WATCH, ensure_topics, event_id, msg, part, profile_id


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), WATCH, RAILS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    watch_p = await (await own_client()).producer(STREAM, WATCH, topic_partitions_count=PARTITIONS[WATCH], mode=cfg)
    rail_p = await (await own_client()).producer(STREAM, RAILS, topic_partitions_count=PARTITIONS[RAILS], mode=cfg)
    # Signal sequence: one counter for all profiles, started from the clock, so each profile's sequence only
    # grows, even across restarts of this script, without keeping a counter per profile.
    next_seq = iter(range(time.time_ns(), 1 << 63))
    watching = {}                           # profile -> live event it is watching now (this script's view)
    pending = []                            # (when, profile, needs, now, left): the next Live rail request
    stats = defaultdict(int)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def signal(profile, now_ns):
        cur = watching.get(profile, "")
        if not cur:
            act, new = "start", event_id(random.randrange(a.events))
        elif random.random() < a.switch_share:
            act, new = "switch", random.choice([event_id(i) for i in random.sample(range(a.events), 2)
                                                if event_id(i) != cur])
        else:
            act, new = "stop", ""
        if new:
            watching[profile] = new
        else:
            watching.pop(profile, None)
        s = next(next_seq)
        stats[act] += 1
        if random.random() < a.request_share:
            heapq.heappush(pending, (time.monotonic() + random.uniform(a.gap_min, a.gap_max), profile, s, new, cur))
        # The message carries the resulting state, not a delta: a reader that sees only the latest signal is right.
        return SendMessage(msg(profile=profile, seq=s, act=act, now=new, left=cur, ns=now_ns))

    def request(profile, needs, now, left, now_ns):
        live = {event_id(i) for i in random.sample(range(a.events), min(a.candidates, a.events))}
        live.update(e for e in (now, left) if e)    # both are airing: the viewer just watched them
        stats["requests"] += 1
        return SendMessage(msg(profile=profile, needs=needs, now=now, left=left, live=sorted(live), ns=now_ns))

    async def send(producer, items, parts):
        # One batch per partition, all sent at once: per-profile order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for profile, m in items:
            groups[part(profile, parts)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def run():
        t0 = due = time.monotonic()
        owed = 0.0
        async with watch_p, rail_p:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.surge if a.surge_at <= t < a.surge_at + a.surge_for else 1
                owed += a.signals_per_s * x / 100
                now = time.time_ns()
                sigs = []
                for _ in range(int(owed)):
                    p = profile_id(random.randrange(a.profiles))
                    sigs.append((p, signal(p, now)))
                owed -= int(owed)
                reqs = []
                while pending and pending[0][0] <= now_s:
                    _, p, needs, cur, left = heapq.heappop(pending)
                    reqs.append((p, request(p, needs, cur, left, time.time_ns())))
                await asyncio.gather(send(watch_p, sigs, PARTITIONS[WATCH]), send(rail_p, reqs, PARTITIONS[RAILS]))
                stats["surge"] = x > 1
                due += 0.01                 # from the due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "surge" if stats["surge"] else "steady"
            n = stats["start"] + stats["switch"] + stats["stop"]
            print(f"[apps] {n:>7,} watch signals/s ({phase})  start {stats['start']:,}  switch {stats['switch']:,}  "
                  f"stop {stats['stop']:,}  Live rail requests {stats['requests']:,}  watching now {len(watching):,}",
                  flush=True)
            for k in ("start", "switch", "stop", "requests"):
                stats[k] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--profiles", type=int, default=5_000_000, help="profiles in the live window (illustrative)")
    p.add_argument("--events", type=int, default=60, help="live events airing at once")
    p.add_argument("--signals-per-s", type=float, default=20_000, help="starts, switches and stops a second")
    p.add_argument("--switch-share", type=float, default=0.65, help="of signals from a profile already watching, switches")
    p.add_argument("--request-share", type=float, default=0.5, help="share of signals followed by a Live rail request")
    p.add_argument("--gap-min", type=float, default=0.3, help="seconds from a signal to the next rail request, at least")
    p.add_argument("--gap-max", type=float, default=2.0, help="seconds from a signal to the next rail request, at most")
    p.add_argument("--candidates", type=int, default=12, help="live events offered on the rail")
    p.add_argument("--surge", type=float, default=2.0, help="rate multiplier while a slate tips off")
    p.add_argument("--surge-at", type=float, default=0, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
