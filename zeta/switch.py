"""Authorization switch: every approval and reversal, on the primary card or a family add-on card,
published as the account's resulting spend.

Plays Tachyon's side of the lane. Each authorization is decided here against the account's true spend
(the source of truth in the kit). When an approval or a reversal changes an account's spend, this script
publishes it, keyed by account:
  * account-spend: the lane. The account's resulting spend today per category (MCC group), across the
    primary card and every add-on card, never a delta, with a per-account sequence number, for the
    transaction feature store the decision engine reads;
  * auth-checks: when an account whose spend changed in the last 10 minutes authorizes again, the decision
    engine's check on that authorization. It carries the sequence of the account's last change, which the
    feature store must already hold, the amount, the add-on card's category limit and the true spend.

Most accounts authorize once in a while. A small share of authorizations comes in runs: one add-on card,
several authorizations in one category, one to three seconds apart, together crossing the category limit
the primary cardholder set (a split bill, a checkout retried, a lost add-on card). Those are the checks
where a late spend update lets an authorization through over the limit. A share of approvals is reversed
a few seconds later, which lowers spend. --peak multiplies the rate for --peak-for seconds.
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
import zlib
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import CATEGORIES, CHECKS, DEFAULT_URL, PARTITIONS, SPEND, STREAM, Spend, account_id, card_id, ensure_topics, msg, part


def addons(account: str) -> int:
    return zlib.crc32(account.encode()) % 4     # 0 to 3 family add-on cards per account


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SPEND, CHECKS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    spend_p = await (await own_client()).producer(STREAM, SPEND, topic_partitions_count=PARTITIONS[SPEND], mode=cfg)
    chk_p = await (await own_client()).producer(STREAM, CHECKS, topic_partitions_count=PARTITIONS[CHECKS], mode=cfg)
    sp = Spend()
    limit = int(a.limit_rupees * 100)       # paise, per add-on card category per day, set by the primary cardholder
    last_change = {}                        # account -> when its spend last changed
    runs = []                               # (when, account, card, category, authorizations left)
    reversals = []                          # (when, account, category, amount)
    stats = defaultdict(int)
    n_auth = 0
    gc.freeze()                             # keep start-up objects out of the collector's way

    def change(account, category, delta, kind, ref, card, now, now_s):
        v = sp.apply(account, category, delta)
        last_change[account] = now_s
        return (SPEND, account, SendMessage(msg(kind=kind, ref=ref, card=card, cat=category, ns=now, **v)))

    def authorize(account, card, category, amount, run, now_s):
        nonlocal n_auth
        n_auth += 1
        now = time.time_ns()
        ref = f"R{n_auth:014d}"
        out = []
        on_addon = not card.endswith("-P")
        lim = limit if on_addon else 0     # 0: no category limit on the primary card
        true_spend = sp.get(account, category)
        if account in last_change and 0.1 <= now_s - last_change[account] < 600:
            # The decision engine's check on this authorization must already see the account's last change.
            out.append((CHECKS, account, SendMessage(msg(account=account, needs=sp.seq[account], cat=category, amount=amount,
                                                         limit=lim, true_spend=true_spend, run=run, ref=ref, ns=now))))
            stats["checks"] += 1
        stats["auths"] += 1
        stats["run"] += run
        if lim and true_spend + amount > lim:
            stats["declined"] += 1          # over the add-on card's category limit: declined, spend unchanged
            return out
        out.append(change(account, category, amount, "APPROVED", ref, card, now, now_s))
        if random.random() < a.reversal_share:
            heapq.heappush(reversals, (now_s + random.uniform(2, 20), account, category, amount, card))
        return out

    async def send(producer, items, parts):
        # One batch per partition, all sent at once: per-account order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for account, m in items:
            groups[part(account, parts)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def run():
        t0 = due = time.monotonic()
        owed = owed_r = 0.0
        async with spend_p, chk_p:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                rate = a.auths_per_s * x / 100
                owed += rate * (1 - a.run_share)
                owed_r += rate * a.run_share / 4.5          # runs average four to five authorizations each
                made = []
                for _ in range(int(owed)):                  # ordinary authorizations: a large pool, few repeats
                    acc = account_id(random.randrange(a.accounts))
                    made += authorize(acc, card_id(acc, random.randint(0, addons(acc))), random.choice(CATEGORIES),
                                      int(random.paretovariate(1.3) * 20_000), 0, now_s)
                owed -= int(owed)
                for _ in range(int(owed_r)):                # a run starts on an add-on card: its first authorization now
                    acc = account_id(a.accounts + random.randrange(a.accounts))
                    card, cat = card_id(acc, random.randint(1, 3)), random.choice(CATEGORIES)
                    made += authorize(acc, card, cat, int(limit * random.uniform(0.25, 0.45)), 1, now_s)
                    heapq.heappush(runs, (now_s + random.uniform(a.gap_min, a.gap_max), acc, card, cat, random.randint(1, 6)))
                owed_r -= int(owed_r)
                while runs and runs[0][0] <= now_s:         # the next authorization of each open run
                    _, acc, card, cat, left = heapq.heappop(runs)
                    made += authorize(acc, card, cat, int(limit * random.uniform(0.25, 0.45)), 1, now_s)
                    if left > 1:
                        heapq.heappush(runs, (now_s + random.uniform(a.gap_min, a.gap_max), acc, card, cat, left - 1))
                while reversals and reversals[0][0] <= now_s:   # a merchant reverses an earlier approval
                    _, acc, cat, amount, card = heapq.heappop(reversals)
                    n = time.time_ns()
                    made.append(change(acc, cat, -amount, "REVERSED", f"V{n}", card, n, now_s))
                    stats["reversed"] += 1
                if made:
                    await asyncio.gather(send(spend_p, [(k, m) for tp, k, m in made if tp == SPEND], PARTITIONS[SPEND]),
                                         send(chk_p, [(k, m) for tp, k, m in made if tp == CHECKS], PARTITIONS[CHECKS]))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "peak" if stats["peak"] else "steady"
            print(f"[switch] {stats['auths']:>7,} authorizations/s ({phase})  in add-on runs {stats['run']:,}  "
                  f"declined at the category limit {stats['declined']:,}  reversals {stats['reversed']:,}  "
                  f"checks on repeat accounts {stats['checks']:,}  accounts tracked {len(sp.spend):,}", flush=True)
            for k in ("auths", "run", "declined", "reversed", "checks"):
                stats[k] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--accounts", type=int, default=25_000_000, help="accounts in the ordinary pool")
    p.add_argument("--auths-per-s", type=float, default=250, help="about 10x the 2024 daily average Zeta reported")
    p.add_argument("--run-share", type=float, default=0.02, help="share of authorizations made in add-on card runs")
    p.add_argument("--reversal-share", type=float, default=0.03, help="share of approvals reversed 2-20 s later")
    p.add_argument("--limit-rupees", type=float, default=10_000, help="add-on card category limit per day, illustrative")
    p.add_argument("--gap-min", type=float, default=1.0, help="seconds between authorizations in a run, at least")
    p.add_argument("--gap-max", type=float, default=3.0, help="seconds between authorizations in a run, at most")
    p.add_argument("--peak", type=float, default=4, help="rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
