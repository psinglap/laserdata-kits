"""The lending system of record: loan events, the user-level aggregate each one changes, and the checkouts that follow.

Plays the source side of the design in Affirm's "Precompute First" post (Oct 2026): the system of record
"sees every event that could change one of these values — a new loan, a manual payment, a dispute opening
or closing", keeps "a monotonically increasing counter per customer", rolls the history up to one
user-level aggregate and publishes it as an event. This script publishes, keyed by customer:
  * user-aggregates: the lane. On every loan event, the customer's new counter and the resulting aggregate
    (total repaid, outstanding balance, loans charged off and those still unpaid, accounts overdue,
    open disputes), never only the change;
  * checkout-reads: a returning customer's checkout. It carries the counter the system of record holds at
    that moment, which is what the decision-time version check compares the stored value with.

At start-up it publishes a snapshot of every returning customer's aggregate, as a backfill would. Some loan
events are followed by a checkout from the same customer a moment to minutes later (a second purchase, or
paying a bill and then shopping); other checkouts come from returning customers with no recent change.
--redeliver resends a small share of earlier aggregates, as an at-least-once pipeline does; the feature
store must count them as duplicate or older and keep the newer value. --peak multiplies the rates.
"""
import argparse
import asyncio
import gc
import heapq
import math
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import AGGREGATES, CHECKOUTS, DEFAULT_URL, PARTITIONS, STREAM, customer_id, ensure_topics, msg, part

# Loan events and how often each happens (illustrative shares; Affirm doesn't publish its mix).
EVENTS = (("new_loan", 0.30), ("autopay_payment", 0.40), ("manual_payment", 0.17), ("dispute_open", 0.04),
          ("dispute_close", 0.03), ("missed_payment", 0.05), ("charge_off", 0.01))
NAMES, WEIGHTS = zip(*EVENTS)


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), AGGREGATES, CHECKOUTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    agg_p = await (await own_client()).producer(STREAM, AGGREGATES, topic_partitions_count=PARTITIONS[AGGREGATES], mode=cfg)
    chk_p = await (await own_client()).producer(STREAM, CHECKOUTS, topic_partitions_count=PARTITIONS[CHECKOUTS], mode=cfg)

    agg = {}                                # customer -> [counter, total_repaid, outstanding, loans, charged_off,
    last_change = {}                        #              charged_off_unpaid, overdue, disputes_open] (cents)
    pending = []                            # (due, n, customer): checkouts to come after a loan event
    recent = deque(maxlen=2000)             # earlier aggregates, for redelivery
    stats = defaultdict(int)
    n_new = [0]

    def returning(i):
        # A returning customer's history so far, made up the same way every run.
        r = random.Random(i)
        loans = r.randint(1, 30)
        repaid = r.randint(5_000, 400_000) * loans
        return [1, repaid, r.randint(0, 150_000), loans, int(r.random() < 0.03), 0, int(r.random() < 0.05), 0]

    def aggregate_msg(u, ev, now, **extra):
        s = agg[u]
        return u, SendMessage(msg(u=u, v=s[0], schema=a.schema, ev=ev, total_repaid=s[1], outstanding=s[2], loans=s[3],
                                  charged_off=s[4], charged_off_unpaid=s[5], overdue=s[6], disputes_open=s[7],
                                  ns=now, **extra))

    def apply(u, ev):
        s = agg[u]
        if ev == "new_loan":
            s[2] += random.randint(5_000, 200_000)
            s[3] += 1
        elif ev in ("autopay_payment", "manual_payment") and s[2] > 0:
            paid = min(s[2], random.randint(2_500, 50_000))
            s[1] += paid
            s[2] -= paid
            if ev == "manual_payment" and s[6]:
                s[6] -= 1                   # paying an overdue installment by hand brings the account current
        elif ev == "dispute_open":
            s[7] += 1
        elif ev == "dispute_close" and s[7]:
            s[7] -= 1
        elif ev == "missed_payment" and s[2] > 0:
            s[6] += 1
        elif ev == "charge_off" and s[6]:
            s[6] -= 1
            s[4] += 1
            s[5] += 1
        s[0] += 1                           # every loan event moves the counter, whether or not a value changed

    async def send(producer, items, parts):
        # One batch per partition, all sent at once: per-customer order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for key, m in items:
            groups[part(key, parts)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def snapshot():
        now = time.time_ns()
        for i in range(0, a.customers, 5000):
            items = []
            for n in range(i, min(a.customers, i + 5000)):
                u = customer_id(n)
                agg[u] = returning(n)
                items.append(aggregate_msg(u, "snapshot", now, snap=1))
            await send(agg_p, items, PARTITIONS[AGGREGATES])
        print(f"[lending] snapshot: {a.customers:,} returning customers' aggregates", flush=True)

    def gap():
        # Seconds from a loan event to the same customer's next checkout: log-uniform, so many come within
        # seconds and some minutes later.
        return math.exp(random.uniform(math.log(a.gap_min), math.log(a.gap_max)))

    async def run():
        t0 = due = time.monotonic()
        owed_ev = owed_chk = 0.0
        n_chk = 0
        async with agg_p, chk_p:
            await snapshot()
            gc.freeze()                     # keep start-up objects (the snapshot) out of the collector's way
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed_ev += a.events_per_s * x / 100
                owed_chk += a.checkouts_per_s * x / 100
                aggs, checkouts, now = [], [], time.time_ns()
                for _ in range(int(owed_ev)):
                    if random.random() < a.new_share:
                        n_new[0] += 1
                        u = f"N{n_new[0]:09d}"           # a first-time customer: their first loan
                        agg[u] = [0, 0, 0, 0, 0, 0, 0, 0]
                        ev = "new_loan"
                        stats["first"] += 1
                    else:
                        u = customer_id(random.randrange(a.customers))
                        ev = random.choices(NAMES, WEIGHTS)[0]
                    apply(u, ev)
                    last_change[u] = now
                    item = aggregate_msg(u, ev, now)
                    aggs.append(item)
                    recent.append(item)
                    stats["events"] += 1
                    if random.random() < a.follow_share:
                        n_chk += 1
                        heapq.heappush(pending, (now_s + gap(), n_chk, u))
                if a.redeliver and recent:
                    for _ in range(int(len(aggs) * a.redeliver + random.random())):
                        u, m = random.choice(recent)
                        aggs.append((u, m))     # the same bytes again, later: a duplicate or an older value
                        stats["redelivered"] += 1
                owed_ev -= int(owed_ev)

                def checkout(u, kind):
                    c_ns = time.time_ns()
                    # The checkout starts now; the system of record's counter for this customer is agg[u][0].
                    checkouts.append((u, SendMessage(msg(u=u, v=agg[u][0], schema=a.schema, kind=kind,
                                                         last_ns=last_change.get(u, 0), ns=c_ns))))
                    stats[kind] += 1

                while pending and pending[0][0] <= now_s:       # follow-up checkouts whose time has come
                    checkout(heapq.heappop(pending)[2], "follow_up")
                for _ in range(int(owed_chk)):
                    checkout(customer_id(random.randrange(a.customers)), "returning")
                owed_chk -= int(owed_chk)
                if aggs or checkouts:
                    await asyncio.gather(send(agg_p, aggs, PARTITIONS[AGGREGATES]),
                                         send(chk_p, checkouts, PARTITIONS[CHECKOUTS]))
                stats["peak"] = x > 1
                due += 0.01                 # from the due time, so one slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "peak" if stats["peak"] else "steady"
            print(f"[lending] {stats['events']:>6,} loan events/s ({phase})  first loans {stats['first']:,}  "
                  f"redelivered {stats['redelivered']:,}  checkouts: after a loan event {stats['follow_up']:,}  "
                  f"returning {stats['returning']:,}  waiting {len(pending):,}", flush=True)
            for k in ("events", "first", "redelivered", "follow_up", "returning"):
                stats[k] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--customers", type=int, default=500_000, help="returning customers in the snapshot")
    p.add_argument("--events-per-s", type=float, default=400, help="loan events per second, all customers")
    p.add_argument("--checkouts-per-s", type=float, default=200, help="checkouts by returning customers with no recent change")
    p.add_argument("--follow-share", type=float, default=0.25, help="share of loan events followed by a checkout")
    p.add_argument("--new-share", type=float, default=0.05, help="share of loan events that are a first-time customer's first loan")
    p.add_argument("--gap-min", type=float, default=0.05, help="seconds from a loan event to the next checkout, at least")
    p.add_argument("--gap-max", type=float, default=300, help="seconds from a loan event to the next checkout, at most")
    p.add_argument("--redeliver", type=float, default=0.01, help="share of aggregates sent again later")
    p.add_argument("--schema", type=int, default=1, help="aggregate schema version")
    p.add_argument("--peak", type=float, default=1, help="rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
