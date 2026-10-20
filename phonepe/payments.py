"""Payments: every completed payment, published as the payer's resulting velocity counts.

Plays the payment flow. When a payment completes, the payer's rolling counts change. This script
publishes them, keyed by payer:
  * payer-velocity: the lane. The payer's resulting counts over 60 s and 10 min (payments, amount,
    distinct payees), never a delta, with a per-payer sequence number, for the aggregate store
    the risk check reads;
  * risk-checks: when a payer who paid in the last 10 minutes starts another payment, the risk
    check on it. It carries the sequence of the payer's previous payment, which the check must count.

Most payers pay once in a while. A small share of payments comes in bursts: one payer, several
payments to new payees, one to three seconds apart (the shape of a scam or account-takeover run).
Those are the checks where a late count matters. --peak multiplies the rate for --peak-for seconds.
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

from common import CHECKS, DEFAULT_URL, PARTITIONS, STREAM, VELOCITY, Velocity, ensure_topics, msg, part, payee_id, payer_id


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), VELOCITY, CHECKS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    vel_p = await (await own_client()).producer(STREAM, VELOCITY, topic_partitions_count=PARTITIONS[VELOCITY], mode=cfg)
    chk_p = await (await own_client()).producer(STREAM, CHECKS, topic_partitions_count=PARTITIONS[CHECKS], mode=cfg)
    vel = Velocity()
    last_paid = {}                          # payer -> when their last payment completed
    bursts = []                             # (when, payer, payments left): the next payment of a burst
    stats = defaultdict(int)
    n_txn = 0
    gc.freeze()                             # keep start-up objects out of the collector's way

    def pay(payer, amount, payee, burst, now_s):
        nonlocal n_txn
        n_txn += 1
        now = time.time_ns()
        txn = f"T{n_txn:014d}"
        out = []
        if payer in last_paid and 0.1 <= now_s - last_paid[payer] < 600:  # nobody pays twice within 100 ms
            # The risk check on this payment must already count the payer's previous one.
            out.append((CHECKS, payer, SendMessage(msg(payer=payer, needs=vel.seq[payer], txn=txn, burst=burst, ns=now))))
            stats["checks"] += 1
        v = vel.add(payer, amount, payee, now_s)
        last_paid[payer] = now_s
        out.append((VELOCITY, payer, SendMessage(msg(txn=txn, ns=now, **v))))
        stats["payments"] += 1
        stats["burst"] += burst
        return out

    async def send(producer, items, parts):
        # One batch per partition, all sent at once: per-payer order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for payer, m in items:
            groups[part(payer, parts)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def run():
        t0 = due = time.monotonic()
        owed = owed_b = 0.0
        async with vel_p, chk_p:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                rate = a.payments_per_s * x / 100
                owed += rate * (1 - a.burst_share)
                owed_b += rate * a.burst_share / 5          # bursts average five payments each
                made = []
                for _ in range(int(owed)):                  # ordinary payments: a large pool, few repeats
                    p = payer_id(random.randrange(a.payers))
                    made += pay(p, int(random.paretovariate(1.3) * 10_000), payee_id(random.randrange(a.payers)), 0, now_s)
                owed -= int(owed)
                for _ in range(int(owed_b)):                # a burst starts: its first payment now
                    p = payer_id(a.payers + random.randrange(a.payers))
                    made += pay(p, random.randrange(200_000, 1_000_000), payee_id(random.randrange(10 ** 9)), 1, now_s)
                    heapq.heappush(bursts, (now_s + random.uniform(a.gap_min, a.gap_max), p, random.randint(2, 7)))
                owed_b -= int(owed_b)
                while bursts and bursts[0][0] <= now_s:     # the next payment of each running burst
                    _, p, left = heapq.heappop(bursts)
                    made += pay(p, random.randrange(200_000, 1_000_000), payee_id(random.randrange(10 ** 9)), 1, now_s)
                    if left > 1:
                        heapq.heappush(bursts, (now_s + random.uniform(a.gap_min, a.gap_max), p, left - 1))
                if made:
                    await asyncio.gather(send(vel_p, [(p, m) for tp, p, m in made if tp == VELOCITY], PARTITIONS[VELOCITY]),
                                         send(chk_p, [(p, m) for tp, p, m in made if tp == CHECKS], PARTITIONS[CHECKS]))
                stats["peak"] = x > 1
                if now_s - vel.last_sweep >= 60:            # about once a minute, forget idle payers
                    vel.sweep(now_s)
                    for p in [p for p, w in last_paid.items() if now_s - w > 600]:
                        del last_paid[p]
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "peak" if stats["peak"] else "steady"
            print(f"[payments] {stats['payments']:>7,} payments/s ({phase})  in bursts {stats['burst']:,}  "
                  f"risk checks on repeat payers {stats['checks']:,}  payers tracked {len(vel.hist):,}", flush=True)
            for k in ("payments", "burst", "checks"):
                stats[k] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--payers", type=int, default=20_000_000, help="payers in the ordinary pool")
    p.add_argument("--payments-per-s", type=float, default=4_200, help="about 360 million payments a day, on average")
    p.add_argument("--burst-share", type=float, default=0.02, help="share of payments made in bursts")
    p.add_argument("--gap-min", type=float, default=1.0, help="seconds between payments in a burst, at least")
    p.add_argument("--gap-max", type=float, default=3.0, help="seconds between payments in a burst, at most")
    p.add_argument("--peak", type=float, default=3, help="rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
