"""The MakeMyTrip apps: travellers on the B2C app and on corporate (B2B) travel search and open hotels, then go
back and tap the search bar.

Plays both segments at once. Each search or hotel view is sent, keyed by user, to:
  * hotel-searches: the lane. B2C and B2B write to the same topic on connections of their own, so the
    last-searched service reads one lane for both segments, with no stream union. Each event carries the
    hotel, the city, the check-in date, the segment, and a sequence number that only grows for each user;
  * searchbar-taps: a short time after an event, the traveller's next tap on the search bar. It carries the
    sequence of the traveller's last search, which the list must already hold, and the hotel that should be
    at the top of the list after it.

Travellers keep a destination for a while and sometimes move on (--switch-share): a traveller comparing Goa
resorts opens a hotel in Udaipur, and the list under the search bar should lead with it.
--gap-min/--gap-max set the time from a search to the next tap; --surge multiplies the rate for --surge-for
seconds (a sale opening, when everyone is searching at once).
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import date, timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (CITIES, DEFAULT_URL, HOTELS_PER_CITY, PARTITIONS, SEARCHES, STREAM, TAPS, ensure_topics, fold,
                    head, hotel, msg, part, user_id)

KINDS = ["hotel_search", "hotel_view"]
KIND_MIX = [0.4, 0.6]                       # illustrative


async def main(a):
    async def own_client():                 # one connection per producer, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SEARCHES, TAPS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    lane = {seg: await (await own_client()).producer(STREAM, SEARCHES, topic_partitions_count=PARTITIONS[SEARCHES],
                                                     mode=cfg) for seg in ("b2c", "b2b")}
    tap_p = await (await own_client()).producer(STREAM, TAPS, topic_partitions_count=PARTITIONS[TAPS], mode=cfg)
    # Search sequence: one counter for all users, started from the clock, so each user's sequence only grows,
    # even across restarts of this script, without keeping a counter per user.
    next_seq = iter(range(time.time_ns(), 1 << 63))
    travellers = {}                         # user -> [destination city, check-in, last-searched list]
    pending = []                            # (when, user, needs, top hotel, new): the next tap on the search bar
    stats = defaultdict(int)
    today = date.today()
    gc.freeze()                             # keep start-up objects out of the collector's way

    def search(user, seg, now_ns):
        t = travellers.get(user)
        if t is None:
            t = travellers[user] = [random.choice(CITIES), None, None]
            t[1] = (today + timedelta(days=random.randint(1, 60))).isoformat()
        if random.random() < a.switch_share:
            t[0] = random.choice(CITIES)    # moved on to another destination
            t[1] = (today + timedelta(days=random.randint(1, 60))).isoformat()
            stats["switch"] += 1
        kind = random.choices(KINDS, KIND_MIX)[0]
        h = hotel(t[0], random.randrange(HOTELS_PER_CITY))
        new = h not in {e[0] for e in t[2] or ()}
        t[2] = fold(t[2], h, t[0], t[1])    # what the list should hold after this search
        seq = next(next_seq)
        stats[kind] += 1
        stats[seg] += 1
        if random.random() < a.tap_share:
            heapq.heappush(pending, (time.monotonic() + random.uniform(a.gap_min, a.gap_max), user, seq,
                                     head(t[2]), new))
        return SendMessage(msg(user=user, seg=seg, seq=seq, kind=kind, hotel=h, city=t[0], checkin=t[1], ns=now_ns))

    def tap(user, needs, top, new, now_ns):
        stats["taps"] += 1
        return SendMessage(msg(user=user, needs=needs, top=top, new=new, ns=now_ns))

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
        async with lane["b2c"], lane["b2b"], tap_p:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.surge if a.surge_at <= t < a.surge_at + a.surge_for else 1
                owed += a.searches_per_s * x / 100
                now = time.time_ns()
                by_seg = {"b2c": [], "b2b": []}
                for _ in range(int(owed)):
                    seg = "b2b" if random.random() < a.b2b_share else "b2c"
                    u = user_id(random.randrange(a.users), seg)
                    by_seg[seg].append((u, search(u, seg, now)))
                owed -= int(owed)
                taps = []
                while pending and pending[0][0] <= now_s:
                    _, u, needs, top, new = heapq.heappop(pending)
                    taps.append((u, tap(u, needs, top, new, time.time_ns())))
                await asyncio.gather(send(lane["b2c"], by_seg["b2c"], PARTITIONS[SEARCHES]),
                                     send(lane["b2b"], by_seg["b2b"], PARTITIONS[SEARCHES]),
                                     send(tap_p, taps, PARTITIONS[TAPS]))
                stats["surge"] = x > 1
                due += 0.01                 # from the due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "surge" if stats["surge"] else "steady"
            n = stats["b2c"] + stats["b2b"]
            print(f"[app] {n:>7,} searches/s ({phase})  B2C {stats['b2c']:,}  B2B {stats['b2b']:,}  "
                  f"hotel searches {stats['hotel_search']:,}  hotel views {stats['hotel_view']:,}  "
                  f"new destinations {stats['switch']:,}  search-bar taps {stats['taps']:,}  "
                  f"travellers {len(travellers):,}", flush=True)
            for k in KINDS + ["b2c", "b2b", "switch", "taps"]:
                stats[k] = 0

    await asyncio.gather(run_loop(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--users", type=int, default=1_000_000, help="travellers searching (illustrative)")
    p.add_argument("--searches-per-s", type=float, default=10_000, help="hotel searches and hotel views a second")
    p.add_argument("--b2b-share", type=float, default=0.15, help="share of events from corporate (B2B) travel")
    p.add_argument("--switch-share", type=float, default=0.2, help="share of events where the traveller moves to another city")
    p.add_argument("--tap-share", type=float, default=0.5, help="share of events followed by a tap on the search bar")
    p.add_argument("--gap-min", type=float, default=0.3, help="seconds from a search to the next tap, at least")
    p.add_argument("--gap-max", type=float, default=3.0, help="seconds from a search to the next tap, at most")
    p.add_argument("--surge", type=float, default=2.0, help="rate multiplier when a sale opens")
    p.add_argument("--surge-at", type=float, default=0, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
