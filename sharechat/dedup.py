"""The feed's dedup service: viewed posts kept current from the lane, and every feed request filtered against them.

Two readers, each on its own connection and consumer group:
  * viewed-posts (the lane): adds each flush's post IDs to the user's recently viewed set,
    ignoring any older or repeated sequence. Flush-to-dedup latency is timed here.
  * feed-requests: plays the dedup step in front of ranking. It drops candidates the user has
    viewed, and asks: did the service already hold the user's last flush when this request started?
    Any just-viewed candidate that got through is a repeat served to the user.

The set covers the minutes before the windowed job persists views to the database (--keep-s);
older views are the database's job, as today. Prints flush-to-dedup p50/p99/p99.9 every second,
with PASS/FAIL against --budget-ms, and the business number: repeats served.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import DEFAULT_URL, REQUESTS, STREAM, VIEWED, Window, ensure_topics, load


async def main(a):
    async def own_client():                 # one connection per reader, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), VIEWED, REQUESTS)

    async def group(topic):
        return await (await own_client()).consumer_group(
            f"{a.name}-{topic}-{os.getpid()}", STREAM, topic, polling_strategy=PollingStrategy.First(), batch_length=5000,
            auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))

    viewed, requests = await group(VIEWED), await group(REQUESTS)
    seen = {}                               # user -> [seq, when applied, viewed post IDs, last flush time]
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays flushes made before it joined
    gc.freeze()

    async def on_viewed(m):
        v = load(m.payload())
        u = v["user"]
        e = seen.get(u)
        if e and v["seq"] <= e[0]:
            stats["dropped"] += 1           # older or repeated flush: already applied
            return
        now = time.time_ns()
        if e is None:
            seen[u] = e = [0, 0, set(), 0]
        e[0], e[1], e[3] = v["seq"], now, time.monotonic()
        e[2].update(v["posts"])
        age = (now - v["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest flush seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_request(m):
        r = load(m.payload())
        if not caught_up[0] or time.time_ns() - r["ns"] > 5e9:
            return                          # replayed history, not a request happening now
        seq, applied, posts, _ = seen.get(r["user"], (0, 0, set(), 0))
        served = [p for p in r["cands"] if p not in posts]          # the dedup step
        stats["requests"] += 1
        stats["removed"] += len(r["cands"]) - len(served)
        # The request started at r["ns"]. Had the service applied the user's last flush by then?
        # (Both clocks are wall clocks; across machines, keep them NTP-synced.)
        late = seq < r["needs"] or (seq == r["needs"] and applied > r["ns"])
        if late:
            stats["late"] += 1
            stats["repeats"] += len(set(served) & set(r["just"]))  # just viewed, served again

    async def sweep():
        while True:
            await asyncio.sleep(30)         # forget users idle past the window the database covers
            cut = time.monotonic() - a.keep_s
            for u in [u for u, e in seen.items() if e[3] < cut]:
                del seen[u]

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of flushes arrived inside the budget; partitions interleave,
                # so one fresh flush alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier flushes this second", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            late = 100 * stats["late"] / stats["requests"] if stats["requests"] else 0
            print(f"[{a.name}] {r['n']:>7,} flushes/s  flush-to-dedup p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  users in set {len(seen):,}", flush=True)
            print(f"[feed] next-page requests {stats['requests']:,}  viewed candidates removed {stats['removed']:,}  "
                  f"missing the last flush {late:.3f}%  repeats served {stats['repeats']:,}", flush=True)
            stats.clear()

    stop = asyncio.Event()
    await asyncio.gather(viewed.consume_messages(on_viewed, stop), requests.consume_messages(on_request, stop),
                         sweep(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="dedup")
    p.add_argument("--budget-ms", type=int, default=50, help="flush-to-dedup p99 budget for the lane")
    p.add_argument("--keep-s", type=float, default=600, help="seconds of views to hold before the database has them")
    asyncio.run(main(p.parse_args()))
