"""Router fleet: every payment attempt's final status, tagged with the release that served it.

Plays the payment router during a staggered release. A stable release serves most traffic; a new
release (the canary) starts at 1% and moves up only when the A/B comparison (guard.py) says SCALE.
For each attempt this script publishes, keyed by payment ID:
  * payment-outcomes: the lane. Payment ID, attempt sequence, release, payment method, merchant,
    final status and error code, with the time the status was final.
It also reads release-verdicts, the guard's decisions, and applies them: SCALE moves the canary's
share up the ladder, ABORT sends all traffic back to the stable release.

At --bad-at seconds the canary develops a defect (illustrative: the card 3DS redirect breaks in the
new release), so most of its card payments fail authentication. The business numbers: how long the
defect served payments before ABORT reached the rollout, and how many payments it failed.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, LADDER, OUTCOMES, PARTITIONS, STREAM, SUCCESS, VERDICTS, ensure_topics, load, msg, part

# Illustrative payment-method mix and success rates; Juspay does not publish these. Set them to yours.
METHODS = (("UPI", 0.55, 0.91), ("CARD", 0.25, 0.88), ("NETBANKING", 0.08, 0.85), ("WALLET", 0.12, 0.95))
FAILURES = (("authorization_failed", "DECLINED"), ("authorization_failed", "INSUFFICIENT_FUNDS"),
            ("failure", "TIMEOUT"), ("authentication_failed", "3DS_FAILED"))


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), OUTCOMES, VERDICTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, OUTCOMES, topic_partitions_count=PARTITIONS[OUTCOMES], mode=cfg)
    verdicts = await (await own_client()).consumer_group(
        f"rollout-{os.getpid()}", STREAM, VERDICTS, polling_strategy=PollingStrategy.First(), batch_length=100,
        auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(milliseconds=1))
    run = f"r{time.time_ns() // 1_000_000}"  # this run's ID, so verdicts from earlier runs are ignored
    st = dict(share=a.start_share, seq=0, defect=False, defect_at=0.0, aborted=False)
    stats, totals = defaultdict(int), defaultdict(int)
    retries = []                            # (when, payment ID, attempt): a failed payment tried again
    n_pay = 0
    gc.freeze()                             # keep start-up objects out of the collector's way

    def attempt(pid, seq, now_s):
        canary = random.random() * 100 < st["share"]
        r, acc = random.random(), 0.0
        for pm, w, ok_rate in METHODS:
            acc += w
            if r < acc:
                break
        bad = canary and st["defect"] and pm == "CARD" and random.random() < a.bad_fail
        if bad:
            status, code = "authentication_failed", "3DS_REDIRECT_ERROR"
        elif random.random() < ok_rate:
            status, code = SUCCESS, None
        else:
            status, code = random.choice(FAILURES)
        if status != SUCCESS and seq == 1 and random.random() < a.retry_share:
            heapq.heappush(retries, (now_s + random.uniform(0.5, 2.0), pid, 2))
        stats["payments"] += 1
        stats["canary"] += canary
        if canary and st["defect"]:
            totals["since_defect"] += 1
            totals["failed_by_defect"] += bad
        return pid, SendMessage(msg(run=run, pid=pid, seq=seq, rel=a.canary if canary else a.stable, canary=int(canary),
                                    pm=pm, mid=f"M{random.randrange(a.merchants):05d}", status=status, code=code,
                                    bad=int(bad)))

    async def send(items):
        # One batch per partition, all sent at once: per-payment order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for pid, m in items:
            groups[part(pid, PARTITIONS[OUTCOMES])].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                               for p, ms in groups.items()))

    async def on_verdict(m):
        v = load(m.payload())
        if v.get("run") != run or v["seq"] <= st["seq"]:
            return                          # an earlier run's verdict, or one already applied
        st["seq"] = v["seq"]
        reach = (time.time_ns() - v["ns"]) / 1e6
        if v["action"] == "ABORT":
            st["share"], st["aborted"] = 0, True
            after = f"{time.monotonic() - st['defect_at']:.2f} s after the defect" if st["defect"] else "before any defect"
            print(f"[router] ABORT applied {after} (verdict reached the rollout in {reach:.1f} ms): canary {a.canary} "
                  f"at 0%; payments on it after the defect {totals['since_defect']:,}, "
                  f"failed by the defect {totals['failed_by_defect']:,}", flush=True)
        elif v["action"] in ("SCALE", "PROMOTE"):
            st["share"] = v["share"]
            print(f"[router] {v['action']} applied: canary {a.canary} at {v['share']}% "
                  f"(verdict reached the rollout in {reach:.1f} ms)", flush=True)

    async def run_traffic():
        nonlocal n_pay
        t0 = due = time.monotonic()
        owed = 0.0
        async with producer:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                if not st["defect"] and t >= a.bad_at and not st["aborted"]:
                    st["defect"], st["defect_at"] = True, now_s
                    print(f"[router] defect starts in canary {a.canary} at {st['share']}% "
                          f"(card 3DS redirect broken, illustrative)", flush=True)
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.payments_per_s * x / 100
                made = []
                for _ in range(int(owed)):
                    n_pay += 1
                    made.append(attempt(f"pay_{run}_{n_pay:010d}", 1, now_s))
                owed -= int(owed)
                while retries and retries[0][0] <= now_s:
                    _, pid, seq = heapq.heappop(retries)
                    made.append(attempt(pid, seq, now_s))
                if made:
                    await send(made)
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "peak" if stats["peak"] else "steady"
            line = (f"[router] {stats['payments']:>7,} payments/s ({phase})  canary {a.canary} at {st['share']}% "
                    f"({stats['canary']:,}/s)")
            if st["defect"] and not st["aborted"]:
                line += (f"  DEFECT LIVE {time.monotonic() - st['defect_at']:.1f} s: payments on it since the defect "
                         f"{totals['since_defect']:,}, failed by the defect {totals['failed_by_defect']:,}")
            print(line, flush=True)
            for k in ("payments", "canary"):
                stats[k] = 0

    await asyncio.gather(run_traffic(), verdicts.consume_messages(on_verdict, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--payments-per-s", type=float, default=3_500, help="about 300 million payment attempts a day")
    p.add_argument("--merchants", type=int, default=5_000)
    p.add_argument("--stable", default="2026.10.14.0", help="the stable release (illustrative name)")
    p.add_argument("--canary", default="2026.10.20.0", help="the new release being staggered (illustrative name)")
    p.add_argument("--start-share", type=float, default=LADDER[0], help="the canary's first share of traffic, percent")
    p.add_argument("--bad-at", type=float, default=200, help="seconds after start when the canary's defect begins")
    p.add_argument("--bad-fail", type=float, default=0.8, help="share of the canary's card payments the defect fails")
    p.add_argument("--retry-share", type=float, default=0.1, help="share of failed payments tried again")
    p.add_argument("--peak", type=float, default=1, help="rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
