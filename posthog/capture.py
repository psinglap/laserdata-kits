"""The source of truth: capture accepting events for every project and writing each one to the live events lane.

--projects projects send --rate events a second in total, skewed the way product traffic is: a few big
projects send most of the events and a long tail sends a few a minute. Each event becomes one compact
record on the lane, keyed by the project's token, with the project's own sequence number, the person's
distinct_id, the event name, the path and the time capture accepted it. The lane carries what Live shows,
not the full event (that still goes to your events topic and ClickHouse as today).

Every --tick seconds the events accepted since the last tick are sent as one batch per partition, all at
once, so a project's events stay in order and the broker sees about one request per partition per tick.
"""
import argparse
import asyncio
import gc
import itertools
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS, LANE, PATHS, STREAM, Stalls, ensure_topics, msg, part, token


async def main(a):
    client = IggyClient.from_connection_string(a.url)     # one connection for this producer
    await client.connect()
    parts = await ensure_topics(client, LANE, partitions={LANE: a.partitions})
    out = await client.producer(STREAM, LANE, topic_partitions_count=parts,
                                mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(microseconds=500)))

    r = random.Random(3)
    tokens = [token(i) for i in range(a.projects)]
    pid = [part(t, parts) for t in tokens]
    weights = [1 / (i + 1) ** a.skew for i in range(a.projects)]          # project 0 is the biggest
    cum = list(itertools.accumulate(weights))
    share = [w / cum[-1] for w in weights]
    users = [max(3, int(s * a.users)) for s in share]                    # people per project
    seq = [0] * a.projects
    stats, stalls = defaultdict(int), Stalls()
    gc.freeze()                                    # keep the projects out of the collector's way

    async def report_line():
        while True:
            await asyncio.sleep(1)
            print(f"[capture] {stats['events']:>7,} events/s  projects sending {stats['projects']:,} of "
                  f"{a.projects:,}  partitions written {stats['parts']}/{parts}  biggest project "
                  f"{stats['top']:,}/s  {stalls.read()}", flush=True)
            stats.clear()

    async def run():
        async with out:
            due, owed, seen = time.monotonic(), 0.0, set()
            next_second = due + 1
            while True:
                owed += a.rate * a.tick
                n = int(owed)
                owed -= n
                now_ns = time.time_ns()
                batches = defaultdict(list)
                for i in r.choices(range(a.projects), cum_weights=cum, k=n):
                    seq[i] += 1
                    batches[pid[i]].append(SendMessage(msg(
                        t=tokens[i], s=seq[i], d=f"u{r.randrange(users[i])}", e=r.choice(EVENTS),
                        p=r.choice(PATHS), ns=now_ns)))
                    seen.add(i)
                    if i == 0:
                        stats["top"] += 1
                await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(p))
                                       for p, ms in batches.items()))
                stats["events"] += n
                stats["parts"] = max(stats["parts"], len(batches))
                if time.monotonic() >= next_second:
                    next_second += 1
                    stats["projects"] = len(seen)
                    seen = set()
                due += a.tick                              # scheduled from the due time, not from "now"
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    await asyncio.gather(run(), report_line(), stalls.watch())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--projects", type=int, default=100000, help="projects sending events")
    ap.add_argument("--rate", type=float, default=50000, help="events a second, all projects together")
    ap.add_argument("--skew", type=float, default=1.0, help="Zipf exponent: how much the big projects dominate")
    ap.add_argument("--users", type=int, default=2000000, help="people across all projects")
    ap.add_argument("--partitions", type=int, default=256, help="lane partitions, used if this creates the topic")
    ap.add_argument("--tick", type=float, default=0.02, help="seconds between sends")
    asyncio.run(main(ap.parse_args()))
