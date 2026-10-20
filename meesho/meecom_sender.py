"""Meecom's communication delivery, fed by the order-update lane.

Two readers, each on its own connection:
  * order updates: sent as soon as they arrive, never rate-limited. A per-order sequence
    drops repeats and anything older than what was already sent (delivery is at least once);
  * campaign sends: drained at --campaign-per-s, the per-client rate limit, so a sale-day
    campaign queues on the broker instead of in front of order updates.

Every sent order update produces a receipt on send-receipts, which order_services.py checks
against the truth. Prints change-to-sender latency for order updates every second, with PASS
or FAIL against --budget-ms, and a [cost] line: the broker's CPU for the messages it
handled, as core-seconds per million messages.
"""
import argparse
import asyncio
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (CAMPAIGN, DEFAULT_URL, ORDER_UPDATES, PARTITIONS, RECEIPTS, STREAM, Window, ensure_topics, load,
                    msg, part)


async def connected(url):
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def main(a):
    admin = await connected(a.url)
    await ensure_topics(admin, ORDER_UPDATES, CAMPAIGN, RECEIPTS)
    started_ns = time.time_ns()
    common = dict(batch_length=5000, auto_commit=AutoCommit.Interval(timedelta(seconds=1)),
                  polling_strategy=PollingStrategy.Next(), init_retries=10, init_retry_interval=timedelta(seconds=1))
    updates = await (await connected(a.url)).consumer_group("meecom-order-updates", STREAM, ORDER_UPDATES,
                                                           poll_interval=timedelta(microseconds=500), **common)
    campaign = await (await connected(a.url)).consumer_group("meecom-campaign", STREAM, CAMPAIGN,
                                                            poll_interval=timedelta(milliseconds=5), **common)
    receipts = await (await connected(a.url)).producer(STREAM, RECEIPTS, topic_partitions_count=PARTITIONS[RECEIPTS])
    last_seq = {}
    lat, camp_wait, stats = Window(), Window(), defaultdict(int)
    pending = defaultdict(list)                      # receipts waiting for the next flush, by partition
    bucket = {"tokens": 0.0, "at": time.monotonic()}

    async def on_update(m):
        u = load(m.payload())
        if u["seq"] <= last_seq.get(u["k"], 0):
            stats["dropped"] += 1                    # a repeat or an older state: never tell the shopper twice
            return
        last_seq[u["k"]] = u["seq"]
        now = time.time_ns()                         # here a real sender hands the message to the channel vendor
        if u["ns"] >= started_ns:
            lat.add((now - u["ns"]) / 1e6)
        stats["sent_" + u["ch"]] += 1
        pending[part(u["k"], PARTITIONS[RECEIPTS])].append(SendMessage(msg(k=u["k"], seq=u["seq"], sent_ns=now)))

    async def on_campaign(m):
        while True:                                  # per-client rate limit: wait for a token
            t = time.monotonic()
            bucket["tokens"] = min(a.campaign_per_s / 10, bucket["tokens"] + (t - bucket["at"]) * a.campaign_per_s)
            bucket["at"] = t
            if bucket["tokens"] >= 1:
                bucket["tokens"] -= 1
                break
            await asyncio.sleep(max(0.001, (1 - bucket["tokens"]) / a.campaign_per_s))
        c = load(m.payload())
        camp_wait.add((time.time_ns() - c["ns"]) / 1e6)
        stats["campaign"] += 1

    async def flush_receipts():
        async with receipts:
            while True:
                await asyncio.sleep(0.01)
                if pending:
                    batch = dict(pending)
                    pending.clear()
                    await asyncio.gather(*(receipts.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                           for pid, ms in batch.items()))

    async def report():
        prev = None
        while True:
            await asyncio.sleep(1)
            r, w = lat.flush(), camp_wait.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            ch = " ".join(f"{c} {stats['sent_' + c]:,}" for c in ("push", "whatsapp", "sms", "email") if stats["sent_" + c])
            print(f"[meecom] {r['n']:>6,} order updates/s  change-to-sender p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  campaign sent {stats['campaign']:,}/s "
                  f"(queued {w['p50'] / 1e3:.1f} s)  repeats dropped {stats['dropped']}  [{ch}]", flush=True)
            stats.clear()
            s = await admin.get_stats()
            if prev is not None:
                written = s.messages_count - prev
                per_m = s.cpu_usage / 100 / written * 1e6 if written else 0
                print(f"[cost]   broker CPU {s.cpu_usage:5.1f}% of one core for {written:,} messages written/s, "
                      f"each read once or more: {per_m:.2f} core-seconds per million messages", flush=True)
            prev = s.messages_count

    stop = asyncio.Event()
    await asyncio.gather(updates.consume_messages(on_update, stop), campaign.consume_messages(on_campaign, stop),
                         flush_receipts(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--budget-ms", type=int, default=200, help="order update, from the change to Meecom's sender")
    p.add_argument("--campaign-per-s", type=float, default=200_000, help="per-client rate limit for campaign sends")
    asyncio.run(main(p.parse_args()))
