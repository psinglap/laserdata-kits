"""A fleet of Computer agent runners: every agent action's credits onto the lane, every pause and resume applied
before the next action.

Plays --runners agent runners in one process. Each one runs agent actions (model calls, searches, browser steps,
sandbox runs) for the tasks of accounts that still have credits and, for every action, publishes to the
agent-actions topic, keyed by account:
  * runner, account, month, a per-(runner, account) sequence, the action kind and the runner's running credits
    for that account (the resulting total, never only the cost of the last action).
Busy accounts run more agents than quiet ones (activity 0.25x to 4x). Each runner also reads credit-decisions on
its own connection and consumer group, and applies them to its own view: PAUSE holds that account's tasks (work
saved, no new actions) until RESUME. A runner never sees another's view, so a pause reaches every runner
through the log.

Prints agent actions and credits per second, actions held while paused, and decision-to-runners p50/p99: the
second hop of the loop.
"""
import argparse
import asyncio
import gc
import itertools
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ACT_HIGH, ACT_LOW, ACTIONS, DECISIONS, DEFAULT_URL, KINDS, PARTITIONS, STREAM, Window, account,
                    activity, ensure_topics, load, msg, part, period)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ACTIONS, DECISIONS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, ACTIONS, topic_partitions_count=PARTITIONS[ACTIONS], mode=cfg)
    start_ns = time.time_ns()
    runners = [f"runner-{i:02d}" for i in range(a.runners)]
    accts = [account(i) for i in range(a.accounts)]
    cum_w = list(itertools.accumulate(activity(u) for u in accts))
    pid = {u: part(u, PARTITIONS[ACTIONS]) for u in accts}
    view = {s: {} for s in runners}         # runner -> {account: (action, ns)}
    seq, cum = defaultdict(int), defaultdict(int)
    lat, stats = Window(), defaultdict(float)

    def reader(s):
        async def on_decision(m):
            d = load(m.payload())
            v = view[s].get(d["a"])
            if v and d["ns"] <= v[1]:
                return                      # older or repeated decision: the newest wins
            view[s][d["a"]] = (d["action"], d["ns"])
            if d["ns"] > start_ns:
                lat.add((time.time_ns() - d["ns"]) / 1e6)   # live decisions only, not ones replayed at start-up
        return on_decision

    readers = []
    for s in runners:
        readers.append((await (await own_client()).consumer_group(
            f"{s}-{os.getpid()}", STREAM, DECISIONS, polling_strategy=PollingStrategy.First(), batch_length=1000,
            auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500)), reader(s)))
    gc.freeze()                             # keep start-up objects out of the collector's way

    def paused(s, u):
        v = view[s].get(u)
        return bool(v) and v[0] == "pause"

    async def action_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        async with producer:
            while True:
                t = time.monotonic() - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.actions_per_s * x / 100
                n = int(owed)
                owed -= n
                p = period(a.period_s)
                batches = defaultdict(list)
                for u in random.choices(accts, cum_weights=cum_w, k=n):
                    s = random.choice(runners)
                    if paused(s, u):
                        stats["held"] += 1      # the task is paused, work saved: no new action
                        continue
                    cr = random.randint(ACT_LOW, ACT_HIGH)
                    seq[(s, u)] += 1
                    cum[(s, u)] += cr
                    batches[pid[u]].append(SendMessage(msg(s=s, a=u, p=p, seq=seq[(s, u)], k=random.choice(KINDS),
                                                           cr=cum[(s, u)])))
                    stats["actions"] += 1
                    stats["credits"] += cr
                # One batch per partition, all sent at once: per-account order is kept, about one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            n_paused = sum(1 for v in view[runners[0]].values() if v[0] == "pause")
            phase = "peak" if stats["peak"] else "steady"
            print(f"[runners] {int(stats['actions']):>7,} agent actions/s ({phase})  credits {stats['credits']:,.0f}/s  "
                  f"held while paused {int(stats['held']):,}  paused in {runners[0]}'s view {n_paused:,}  "
                  f"decision-to-runners n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("actions", "credits", "held"):
                stats[k] = 0

    await asyncio.gather(action_loop(), report(), *(c.consume_messages(cb, asyncio.Event()) for c, cb in readers))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--runners", type=int, default=8, help="agent runners, each with its own decisions reader")
    p.add_argument("--accounts", type=int, default=20_000, help="must match credit_ledger.py")
    p.add_argument("--actions-per-s", type=float, default=10_000, help="agent actions per second, all runners")
    p.add_argument("--period-s", type=float, default=60, help="seconds per compressed month; must match credit_ledger.py")
    p.add_argument("--peak", type=float, default=1, help="action-rate multiplier during the peak (a launch day)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
