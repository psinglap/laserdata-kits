"""The source of truth: every card account's spendable balance and card status, and the primary authorizer.

Each change to an account goes to the card-state topic, keyed by account, with a per-account sequence number and
the resulting state (spendable balance in cents and card status), never only a delta:
  * deposit, transfer_out, card_posted: money in and out of the account;
  * buy_order, sell_settled: cash moving into and out of stocks (more of these at the market open, --open);
  * card_locked, card_unlocked: the customer locks or unlocks the card in the app.

It also plays the card network and processor: it sends authorization requests to auth-requests, and the primary
decides each one at that moment from the true state (the "pull" model). Some requests follow a change on the
same account a moment later (--follow-share, --follow-delay): a purchase right after a deposit, a purchase the
cash that just went into stocks can no longer cover, or someone tapping a card that was just locked.

The stand-in authorizers (standin.py) decide the same requests from their cache of the card-state lane and send
their decisions to standin-decisions. Every second this prints how many stand-in decisions disagreed with the
primary, and how many a cache fed --lag seconds behind would have got wrong.
"""
import argparse
import asyncio
import gc
import heapq
import itertools
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, Partitioning, PollingStrategy, SendMessage

from common import (DECISIONS, DEFAULT_URL, PARTITIONS, REQUESTS, STATE, STREAM, account, connect, decide,
                    ensure_topics, load, msg, part_of)


async def main(a):
    admin = await connect(a.url)
    await ensure_topics(admin, STATE, REQUESTS, DECISIONS)
    lane, req_w = await connect(a.url), await connect(a.url)          # one connection per writer and reader
    decisions = await (await connect(a.url)).consumer_group(
        "primary-compare", STREAM, DECISIONS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))

    keys = [account(i) for i in range(a.accounts)]
    rng = random.Random(7)
    # Spendable balance in cents: many accounts hold little cash, which is where decisions are close.
    avail = {k: rng.choice((rng.randint(0, 2_000), rng.randint(2_000, 30_000), rng.randint(30_000, 500_000)))
             for k in keys}
    status = {k: "locked" if rng.random() < 0.03 else "active" for k in keys}
    seq = defaultdict(int)
    primary = {}                             # request id -> (primary decision, state version, created ns)
    follow = []                              # heap of (due monotonic time, n, account, amount) follow-up purchases
    st, n_req, tie = defaultdict(int), [0], itertools.count()

    def change(k, kind):
        seq[k] += 1
        return k, SendMessage(msg(k=k, seq=seq[k], avail=avail[k], status=status[k], kind=kind))

    def batches(topic, items):
        # One batch per partition: an account's messages stay in order in one partition.
        out = defaultdict(list)
        for k, m in items:
            out[part_of(k, PARTITIONS[topic])].append(m)
        return out.items()

    async def send(writer, topic, items):
        await asyncio.gather(*(writer.send_messages(STREAM, topic, Partitioning.partition_id(p), ms)
                               for p, ms in batches(topic, items)))

    def schedule(k, amount):
        if rng.random() < a.follow_share:
            heapq.heappush(follow, (time.monotonic() + rng.uniform(*a.follow_delay), next(tie), k, max(100, amount)))

    def one_change(k, market):
        r = rng.random()
        before = avail[k]
        if market or r < 0.12:               # cash into stocks, or a sale settling back to cash
            if rng.random() < 0.6 and before > 500:
                avail[k] -= rng.randint(before // 4, before)
                schedule(k, rng.randint(avail[k] + 1, before))           # now more than the account can cover
                return change(k, "buy_order")
            avail[k] += rng.randint(1_000, 200_000)
            schedule(k, rng.randint(before + 1, avail[k]))               # only the new balance covers it
            return change(k, "sell_settled")
        if r < 0.40:
            avail[k] += rng.randint(1_000, 150_000)
            schedule(k, rng.randint(before + 1, avail[k]))
            return change(k, "deposit")
        if r < 0.55 and before > 0:
            avail[k] -= rng.randint(1, before)
            schedule(k, rng.randint(avail[k] + 1, before))
            return change(k, "transfer_out")
        if r < 0.94 and before > 0:
            avail[k] -= min(before, int(rng.lognormvariate(7.5, 1.0)))   # a purchase posting, ~$18 median
            return change(k, "card_posted")
        if status[k] == "active":
            status[k] = "locked"             # lost card, or a customer who locks it between purchases
            schedule(k, rng.randint(500, 8_000))
            return change(k, "card_locked")
        status[k] = "active"
        schedule(k, rng.randint(500, min(8_000, max(500, before))))
        return change(k, "card_unlocked")

    def request(k, amount, kind):
        n_req[0] += 1
        rid = f"r{n_req[0]}"
        primary[rid] = (decide(avail[k], status[k], amount), seq[k], time.time_ns(), kind)
        st["requests"] += 1
        return k, SendMessage(msg(id=rid, k=k, amt=amount))

    async def snapshot():
        # The starting state of every account, like a CDC snapshot (seq 1, not timed as a live change).
        for i in range(0, len(keys), 5000):
            await send(lane, STATE, [change(k, "snapshot") for k in keys[i:i + 5000]])
        print(f"[ledger] snapshot: {len(keys):,} card accounts", flush=True)

    async def run():
        await snapshot()
        gc.freeze()                          # keep start-up tables out of the collector's way
        t0 = due = time.monotonic()
        owed_c = owed_r = 0.0
        while True:
            t = time.monotonic() - t0
            opening = a.open_at and a.open_at <= t < a.open_at + a.open_for
            owed_c += a.rate * (a.open if opening else 1) / 100
            owed_r += a.auths / 100
            changes = []
            for _ in range(int(owed_c)):
                changes.append(one_change(rng.choice(keys), opening and rng.random() < 1 - 1 / a.open))
            owed_c -= int(owed_c)
            reqs, now = [], time.monotonic()
            while follow and follow[0][0] <= now:
                _, _, k, amount = heapq.heappop(follow)
                reqs.append(request(k, amount, "follow"))
            for _ in range(int(owed_r)):
                reqs.append(request(rng.choice(keys), int(rng.lognormvariate(7.5, 1.0)), "other"))
            owed_r -= int(owed_r)
            # The tick's state changes and requests go out together, each writer on its own connection.
            t_send = time.monotonic()
            await asyncio.gather(send(lane, STATE, changes), send(req_w, REQUESTS, reqs))
            st["send_ms"] = max(st["send_ms"], 1000 * (time.monotonic() - t_send))
            st["late_ms"] = max(st["late_ms"], 1000 * (t_send - due))    # how late this tick started
            st["changes"] += len(changes)
            st["open"] = int(bool(opening))
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def on_decision(m):
        d = load(m.payload())
        p = primary.pop(d["id"], None)
        if p is None:
            return                           # decided twice (at-least-once) or already expired
        ok, version, _, kind = p
        st["compared"] += 1
        st[f"compared_{kind}"] += 1
        if d["seq"] < version:
            st["stale"] += 1                 # the stand-in's cache had not yet applied a change made before it
        if d["ok"] != ok:
            st["approved_bad" if d["ok"] else "declined_bad"] += 1
        if d["lok"] != ok:
            st["lag_approved_bad" if d["lok"] else "lag_declined_bad"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            cutoff = time.time_ns() - 5_000_000_000
            for rid in [r for r, p in primary.items() if p[2] < cutoff]:
                del primary[rid]
                st["undecided"] += 1         # no stand-in decision within 5 s
            c = st["compared"]
            wrong = st["approved_bad"] + st["declined_bad"]
            lag = st["lag_approved_bad"] + st["lag_declined_bad"]
            print(f"[ledger] {st['changes']:>6,} changes/s ({'open' if st['open'] else 'steady'})  "
                  f"requests {st['requests']:,}  stand-in decisions compared {c:,} (follow-ups {st['compared_follow']:,})  "
                  f"on stale state {st['stale']:,}  disagreed with the primary {wrong:,} "
                  f"(approved {st['approved_bad']:,} declined {st['declined_bad']:,})  "
                  f"a cache {a.lag:g}s behind would disagree on {lag:,} "
                  f"(approved {st['lag_approved_bad']:,} declined {st['lag_declined_bad']:,})  "
                  f"undecided {st['undecided']:,}  tick late max {st['late_ms']:.0f}ms  send max {st['send_ms']:.0f}ms",
                  flush=True)
            st.clear()

    await asyncio.gather(run(), decisions.consume_messages(on_decision, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--accounts", type=int, default=500_000, help="card accounts")
    p.add_argument("--rate", type=float, default=3_000, help="account changes per second")
    p.add_argument("--auths", type=float, default=500, help="authorization requests per second, besides follow-ups")
    p.add_argument("--follow-share", type=float, default=0.3, help="share of changes followed by a purchase on that account")
    p.add_argument("--follow-delay", type=float, nargs=2, default=(0.15, 2.0), metavar=("MIN", "MAX"),
                   help="seconds between the change and the follow-up purchase")
    p.add_argument("--open", type=float, default=1.0, help="change-rate multiplier at the market open")
    p.add_argument("--open-at", type=float, default=0, help="seconds after start")
    p.add_argument("--open-for", type=float, default=60)
    p.add_argument("--lag", type=float, default=0.5, help="the comparison cache's lag, as set on standin.py (label only)")
    asyncio.run(main(p.parse_args()))
