"""Meecom's communication delivery, fed by the order-update lane.

Two readers, each on its own connection (run them as separate processes with --only, as
you would run transactional and campaign workers):
  * order updates: sent as soon as they arrive, never rate-limited. A per-order sequence
    drops repeats and anything older than what was already sent (delivery is at least once).
    Every sent update produces a receipt on send-receipts, which order_services.py checks
    against the truth;
  * campaign sends: drained at --campaign-per-s, the per-client rate limit, so a sale-day
    campaign queues on the broker instead of in front of order updates.

Prints change-to-sender latency for order updates every second, with PASS or FAIL against
--budget-ms, how fast the campaign topic is growing next door, and the broker's CPU.
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
    lat, camp_wait, stats = Window(), Window(), defaultdict(int)
    last_seq = {}
    pending = defaultdict(list)                      # receipts waiting for the next flush, by partition
    bucket = {"tokens": 0.0, "at": time.monotonic()}
    tasks = []

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

    async def flush_receipts(receipts):
        async with receipts:
            while True:
                await asyncio.sleep(0.01)
                if pending:
                    batch = dict(pending)
                    pending.clear()
                    await asyncio.gather(*(receipts.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                           for pid, ms in batch.items()))

    async def count(topic):
        t = await admin.get_topic(STREAM, topic)
        return t.messages_count if t else 0

    async def report():
        prev = None
        while True:
            await asyncio.sleep(1)
            if a.only in ("both", "orders"):
                r = lat.flush()
                ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
                ch = " ".join(f"{c} {stats['sent_' + c]:,}" for c in ("push", "whatsapp", "sms", "email") if stats["sent_" + c])
                st = await admin.get_stats()          # CPU is measured since the previous call, so one call a second
                now = (await count(CAMPAIGN), st.messages_count)
                grow = now[0] - prev[0] if prev else 0
                print(f"[meecom] {r['n']:>6,} order updates/s  change-to-sender p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                      f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  campaign topic +{grow:,}/s  "
                      f"repeats dropped {stats['dropped']}  [{ch}]", flush=True)
                if prev:
                    print(f"[broker] CPU {st.cpu_usage:5.1f}% of one core  {now[1] - prev[1]:,} messages written/s "
                          f"on all topics", flush=True)
                prev = now
            if a.only in ("both", "campaign"):
                w = camp_wait.flush()
                print(f"[meecom-campaign] {stats['campaign']:>6,} campaign sends/s (limit {a.campaign_per_s:,.0f}/s)  "
                      f"waited in the queue p50 {w['p50'] / 1e3:5.1f} s  max {w['max'] / 1e3:5.1f} s", flush=True)
            stats.clear()

    stop = asyncio.Event()
    if a.only in ("both", "orders"):
        updates = await (await connected(a.url)).consumer_group("meecom-order-updates", STREAM, ORDER_UPDATES,
                                                               poll_interval=timedelta(microseconds=500), **common)
        receipts = await (await connected(a.url)).producer(STREAM, RECEIPTS, topic_partitions_count=PARTITIONS[RECEIPTS])
        tasks += [updates.consume_messages(on_update, stop), flush_receipts(receipts)]
    if a.only in ("both", "campaign"):
        campaign = await (await connected(a.url)).consumer_group("meecom-campaign", STREAM, CAMPAIGN,
                                                                poll_interval=timedelta(milliseconds=5), **common)
        tasks.append(campaign.consume_messages(on_campaign, stop))
    await asyncio.gather(*tasks, report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--only", choices=("both", "orders", "campaign"), default="both", help="which reader this process runs")
    p.add_argument("--budget-ms", type=int, default=200, help="order update, from the change to Meecom's sender")
    p.add_argument("--campaign-per-s", type=float, default=200_000, help="per-client rate limit for campaign sends")
    asyncio.run(main(p.parse_args()))
