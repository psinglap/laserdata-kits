"""Payment services emitting billable usage: one usage message per successful payment, onto the usage lane.

Plays the services that today publish usage to the Kill Bill Kafka consumer plugin's topic. Every successful
payment becomes one message in the plugin's own shape (subscriptionExternalKey, trackingId, tenantId and
unitUsageRecords with txnCount 1 and txnVolume = the payment amount in paise, for the IST recordDate), keyed by
subscription, plus the lane fields: a per-subscription sequence and the subscription's txnCount and txnVolume
for the day so far (the resulting totals, never only this payment).

At-least-once on purpose: --resend-pct of messages are sent a second time, as a service that timed out waiting
for an acknowledgement would. The meter and the Kill Bill writer must count each one once.

A skewed merchant mix: a few large subscriptions carry much of the volume, many small ones a little.
Prints payments, usage messages, volume and resends per second.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (DEFAULT_URL, PARTITIONS, STREAM, USAGE, ensure_topics, part, record_date, rupees,
                    subscription, usage_msg)

TICK = 0.01


async def main(a):
    client = IggyClient.from_connection_string(a.url)      # one connection for this producer
    await client.connect()
    await ensure_topics(client, USAGE)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await client.producer(STREAM, USAGE, topic_partitions_count=PARTITIONS[USAGE], mode=cfg)
    subs = [subscription(i) for i in range(a.subscriptions)]
    pids = [part(s, PARTITIONS[USAGE]) for s in subs]
    seq = defaultdict(int)                  # subscription -> last sequence sent
    day_tot = {}                            # subscription -> (recordDate, txnCount, txnVolume) for the day so far
    recent = deque(maxlen=4000)             # recently sent usage, for the at-least-once resends
    stats = defaultdict(float)
    n = [0]
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def pay():
        t0 = time.monotonic()
        nxt, owed = t0, 0.0
        async with producer:
            while True:
                t = time.monotonic() - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                stats["peak"] = x > 1
                owed += a.tps * x * TICK
                k, owed = int(owed), owed - int(owed)
                now = time.time_ns()
                day = record_date(now)
                batches = defaultdict(list)
                for _ in range(k):
                    i = int(a.subscriptions * random.random() ** 3)
                    sub = subs[i]
                    paise = max(100, int(random.lognormvariate(a.ticket_ln, 1.1)) * 100)  # whole rupees
                    d, c, v = day_tot.get(sub, (day, 0, 0))
                    if d != day:
                        c, v = 0, 0                 # a new IST day: the day's totals start again
                    c, v = c + 1, v + paise
                    day_tot[sub] = (day, c, v)
                    seq[sub] += 1
                    n[0] += 1
                    b = usage_msg(sub, f"cfpay_{n[0]:011d}", day, paise, seq[sub], c, v, now)
                    batches[pids[i]].append(SendMessage(b))
                    stats["sent"] += 1
                    stats["paise"] += paise
                    if a.resend_pct and random.random() < a.resend_pct / 100 and recent:
                        q, old = random.choice(recent)    # a retry of an earlier message: same trackingId, same seq
                        batches[q].append(SendMessage(old))
                        stats["resent"] += 1
                    recent.append((pids[i], b))
                # One batch per partition, all sent at once: a subscription's usage stays in order, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                nxt += TICK
                await asyncio.sleep(max(0.0, nxt - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "peak" if stats["peak"] else "steady"
            print(f"[payments] {int(stats['sent']):>7,} usage msgs/s ({phase})  payments volume "
                  f"{rupees(stats['paise'])}/s  subscriptions active today {len(day_tot):,}  "
                  f"resent {int(stats['resent']):,}", flush=True)
            for k in ("sent", "paise", "resent"):
                stats[k] = 0

    await asyncio.gather(pay(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--tps", type=float, default=4000, help="successful payments per second across all services")
    p.add_argument("--subscriptions", type=int, default=200_000, help="usage-billed subscriptions (merchants)")
    p.add_argument("--ticket-ln", type=float, default=6.2, help="log of the median payment in rupees (6.2 = ~₹490)")
    p.add_argument("--resend-pct", type=float, default=0.5, help="share of usage messages sent twice (retries)")
    p.add_argument("--peak", type=float, default=1, help="payment-rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
