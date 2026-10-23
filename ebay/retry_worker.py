"""The retry worker: pushes whose endpoint did not acknowledge, retried on their own topic.

Reads push-retry in a consumer group (offsets stored once a second), holds each push until it is due, and pushes
it again (simulated, with the same planted outages as push_workers.py). Acknowledged: done. Not acknowledged: back
onto push-retry with the next attempt and a doubled delay (up to --max-backoff-s), so a restarted retry worker
loses nothing. eBay's public docs say an unacknowledged account-deletion notification is resent until it is
acknowledged and the endpoint is marked down after 24 hours; --mark-down-s stands in for that rule.

A down endpoint only ever waits here: the push workers carry on with the next notification and the next window.
Prints, every second, the pushes retried, acknowledged after retry (with how long they were held) and waiting.
"""
import argparse
import asyncio
import gc
import heapq
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, PARTITIONS, RETRY, STREAM, Window, endpoint_down, ensure_topics, load, msg, part


async def main(a):
    async def own_client():
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), RETRY)
    cfg = DirectProducerConfig(batch_length=5000, linger_time=timedelta(microseconds=200))
    out = await (await own_client()).producer(STREAM, RETRY, topic_partitions_count=PARTITIONS[RETRY], mode=cfg)
    group = await (await own_client()).consumer_group(
        "push-retry-workers", STREAM, RETRY, polling_strategy=PollingStrategy.First(), batch_length=2000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(milliseconds=5))
    due, again = [], []                     # held pushes (due ns, n, record); pushes to put back on the topic
    held, stats = Window(), defaultdict(float)
    n = [0]
    gc.freeze()

    async def on_retry(m):
        d = load(m.payload())
        n[0] += 1
        heapq.heappush(due, (d["due"], n[0], d))
        stats["queued"] += 1

    async def retry_loop():
        tick = time.monotonic()
        while True:
            now_ns = time.time_ns()
            now = now_ns / 1e9
            while due and due[0][0] <= now_ns:
                _, _, d = heapq.heappop(due)
                stats["retried"] += 1
                if not endpoint_down(d["i"], now, a.down_share, a.up_s, a.down_s):
                    stats["acked"] += 1
                    held.add((now_ns - d["first"]) / 1e9)
                elif now_ns - d["first"] > a.mark_down_s * 1e9:
                    stats["marked_down"] += 1
                else:
                    d["attempt"] += 1
                    d["due"] = now_ns + int(min(a.max_backoff_s, a.backoff_s * 2 ** (d["attempt"] - 1)) * 1e9)
                    again.append(d)
            tick += 0.05
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def send_again():
        async with out:
            while True:
                await asyncio.sleep(0.02)
                if again:
                    batch = again[:]
                    again.clear()
                    groups = defaultdict(list)
                    for d in batch:
                        groups[part(d["s"], PARTITIONS[RETRY])].append(SendMessage(msg(**d)))
                    await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in groups.items()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = held.flush()
            h = f"held p50 {r['p50']:.1f}s max {r['max']:.1f}s" if r["n"] else "held -"
            print(f"[retry] read {int(stats['queued']):,}  retried {int(stats['retried']):,}  acknowledged after retry "
                  f"{int(stats['acked']):,} ({h})  waiting {len(due):,}  marked down {int(stats['marked_down'])}",
                  flush=True)
            stats.clear()

    await asyncio.gather(retry_loop(), send_again(), report(), group.consume_messages(on_retry, asyncio.Event()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--down-share", type=float, default=0.01, help="must match push_workers.py")
    p.add_argument("--down-s", type=float, default=6, help="must match push_workers.py")
    p.add_argument("--up-s", type=float, default=14, help="must match push_workers.py")
    p.add_argument("--backoff-s", type=float, default=1, help="first retry delay (push_workers.py sets the first)")
    p.add_argument("--max-backoff-s", type=float, default=8)
    p.add_argument("--mark-down-s", type=float, default=24 * 3600, help="give up and mark the endpoint down")
    asyncio.run(main(p.parse_args()))
