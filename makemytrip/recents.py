"""The last-searched service: each traveller's last-searched hotels, kept current from the lane, and every tap on
the search bar served from it.

Two readers, each reading every partition of its topic directly on connections of its own (no consumer group:
a group reader visits one partition per poll, so with 64 partitions a record waits its turn):
  * hotel-searches (the lane, B2C and B2B together): folds each search or view into the traveller's list,
    newest first, ignoring any older or repeated sequence. Search-to-list latency is timed here. In
    production this is where the list would be written to the store the app reads (Aerospike, Redis).
  * searchbar-taps: plays the search bar. It asks: did the list already hold the traveller's last search when
    the tap happened? If not, the traveller was shown a stale list, and if the hotel they just searched was
    not in it at all, they had to search for it again.

--hold-ms is a what-if: apply each search only once it is that old, as a micro-batch path would.
Prints search-to-list p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business numbers:
taps served without the last search, and taps where the hotel just searched was missing from the list.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, PARTITIONS, SEARCHES, STREAM, TAPS, Window, ensure_topics, fold, load


async def main(a):
    async def own_client():                 # one connection per reader, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SEARCHES, TAPS)

    recent = {}                             # user -> [[hotel, city, check-in], ...] newest first
    seen = {}                               # user -> [seq, when applied (ns), last seen (monotonic)]
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays searches made before it joined
    held = deque()                          # what-if only: searches waiting until they are --hold-ms old
    gc.freeze()

    def apply(v):
        u = v["user"]
        s = seen.get(u)
        if s and v["seq"] <= s[0]:
            stats["dropped"] += 1           # older or repeated search: already applied
            return
        recent[u] = fold(recent.get(u), v["hotel"], v["city"], v["checkin"])
        now = time.time_ns()
        if s is None:
            seen[u] = s = [0, 0, 0.0]
        s[0], s[1], s[2] = v["seq"], now, time.monotonic()
        age = (now - v["ns"]) / 1e6
        stats[v["seg"]] += 1
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest search seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_search(m):
        v = load(m.payload())
        if a.hold_ms:
            held.append(v)
        else:
            apply(v)

    async def release():                    # what-if only: apply held searches once they are old enough
        while True:
            cut = time.time_ns() - a.hold_ms * 1_000_000
            while held and held[0]["ns"] <= cut:
                apply(held.popleft())
            await asyncio.sleep(0.002)

    async def on_tap(m):
        r = load(m.payload())
        if not caught_up[0] or time.time_ns() - r["ns"] > 5e9:
            return                          # replayed history, not a tap happening now
        seq, applied, _ = seen.get(r["user"], (0, 0, 0))
        stats["taps"] += 1
        stats["new"] += r["new"]
        # The tap happened at r["ns"]. Had the list applied the traveller's last search by then?
        # (Both clocks are wall clocks; across machines, keep them NTP-synced.)
        if seq < r["needs"] or (seq == r["needs"] and applied > r["ns"]):
            stats["stale"] += 1
            if r["top"] not in {e[0] for e in recent.get(r["user"], ())}:
                stats["missing"] += 1       # the hotel they just searched is not in the list at all

    async def sweep():
        while True:
            await asyncio.sleep(30)         # forget travellers idle past --keep-s (the store keeps them)
            cut = time.monotonic() - a.keep_s
            for u in [u for u, s in seen.items() if s[2] < cut]:
                del seen[u]
                recent.pop(u, None)

    async def report():
        while True:
            await asyncio.sleep(1)
            # Move what survived the last second (lists, travellers) out of the collector's reach, so a full
            # collection never walks hundreds of thousands of long-lived objects and pauses the readers.
            gc.freeze()
            if not caught_up[0]:
                # caught up once a whole second of searches arrived inside the budget; partitions interleave,
                # so one fresh search alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms + a.hold_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier searches this second", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms + a.hold_ms else "FAIL"
            stale = 100 * stats["stale"] / stats["taps"] if stats["taps"] else 0
            print(f"[{a.name}] {r['n']:>7,} searches/s  search-to-list p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms + a.hold_ms}ms)  B2C {stats['b2c']:,}  "
                  f"B2B {stats['b2b']:,}  travellers {len(recent):,}", flush=True)
            print(f"[search-bar] taps {stats['taps']:,}  after a new hotel {stats['new']:,}  "
                  f"served without the last search {stale:.3f}%  last-searched hotel missing {stats['missing']:,}",
                  flush=True)
            stats.clear()

    async def read(topic, handler, pids):
        # Each partition in turn, from the start of the log (the lists are rebuilt from it), then from this
        # reader's own position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{topic}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, topic, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        await handler(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def readers(topic, handler, n):
        parts = list(range(PARTITIONS[topic]))
        return [read(topic, handler, parts[r::n]) for r in range(n)]

    await asyncio.gather(*readers(SEARCHES, on_search, a.readers), *readers(TAPS, on_tap, 2),
                         release(), sweep(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="recents")
    p.add_argument("--budget-ms", type=int, default=50, help="search-to-list p99 budget for the lane")
    p.add_argument("--hold-ms", type=int, default=0, help="what-if: apply each search only once it is this old")
    p.add_argument("--readers", type=int, default=4, help="connections reading the lane's 64 partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--keep-s", type=float, default=3600, help="seconds to keep an idle traveller's list in memory")
    asyncio.run(main(p.parse_args()))
