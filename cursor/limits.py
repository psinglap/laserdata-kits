"""The spend-limit service, fed by the usage lane.

Reads the usage topic directly, partition by partition, on --readers connections of its own
(no consumer group: a group reader visits one partition per poll, so with 16 partitions a record waited for its
partition's turn). Each record is one API server's running
paid usage for one user in the current billing period (the resulting total in USD, with a sequence), so a
user's usage is the sum of the latest totals from every API server, and replays or repeats change nothing.

For every user it applies the limit hierarchy (team default, group override, member override; the highest
applicable limit wins) and decides, as usage arrives:
  * ALERT once usage reaches --alert-at of the limit (informational, usage continues);
  * STOP the moment usage reaches the limit: paid additional usage stops for that user, until the next period;
and publishes each change on limit-decisions, which every API server reads (api_servers.py).

Prints usage-to-limit-check p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business
number: usage past the limit, the USD served to users after they had already reached their spend limit,
because the usage or the stop was still in flight.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (DECISIONS, DEFAULT_URL, PARTITIONS, STREAM, USAGE, Window, ensure_topics, limit, load, msg,
                    period, user)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), USAGE, DECISIONS)
    out = await (await own_client()).producer(STREAM, DECISIONS, topic_partitions_count=PARTITIONS[DECISIONS])
    users = [user(i) for i in range(a.users)]
    limits = {u: limit(u, a.requests_per_s, a.users, a.period_s) for u in users}
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (API server, user) -> last sequence applied
    total = defaultdict(float)              # (period, user) -> usage: sum of every API server's latest total
    last = {}                               # (period, API server, user) -> that server's latest total
    state = {}                              # (period, user) -> "alert" | "stop"
    dseq = defaultdict(int)                 # user -> decision sequence
    pending = []                            # decisions waiting for the next send (one batch per tick)
    past = defaultdict(float)               # period -> USD served after the user reached the limit
    stopped = defaultdict(int)              # period -> users stopped
    by_src = defaultdict(int)               # period -> stops by the source of the winning limit
    caught_up = [False]                     # a new reader first replays usage from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def decide(p, u, new):
        if state.get((p, u)) == new:
            return
        state[(p, u)] = new
        dseq[u] += 1
        pending.append(SendMessage(msg(u=u, p=p, seq=dseq[u], action=new)))
        if new == "stop":
            stopped[p] += 1
            by_src[(p, limits[u][1])] += 1

    def on_usage(m):
        s = load(m.payload())
        now = time.time_ns()
        age = (now - s["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = (s["s"], s["u"])
        if s["seq"] <= seq.get(k, 0):
            stats["dropped"] += 1           # older or repeated total: the newest wins
            return
        seq[k] = s["seq"]
        p, u = s["p"], s["u"]
        before = total[(p, u)]
        total[(p, u)] += s["usd"] - last.get((p, s["s"], u), 0.0)
        last[(p, s["s"], u)] = s["usd"]
        stats["events"] += 1
        cap = limits[u][0]
        if before >= cap:
            past[p] += total[(p, u)] - before   # served after the user had already reached the limit
        if p != period(a.period_s, now / 1e9):
            return                          # an earlier period: count it, but decide nothing
        if total[(p, u)] >= cap:
            decide(p, u, "stop")
        elif total[(p, u)] >= a.alert_at * cap and (p, u) not in state:
            decide(p, u, "alert")

    async def send_decisions():
        # Decisions are small and come in bursts; one batch every 2 ms keeps them off a send-per-decision loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["decisions"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, USAGE, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_usage(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of usage arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier usage records this second",
                      flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            p = period(a.period_s)
            used = sum(v for (pp, _), v in total.items() if pp == p)
            print(f"[{a.name}] {r['n']:>7,} usage events/s  usage-to-limit-check p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"decisions sent {int(stats['decisions']):,}", flush=True)
            print(f"[spend] period {p % 10000}: paid usage ${used:,.2f}  users stopped {stopped[p]:,}/{a.users:,} "
                  f"(team {by_src[(p, 'team')]:,} group {by_src[(p, 'group')]:,} member {by_src[(p, 'member')]:,})  "
                  f"alerted {sum(1 for (pp, _), v in state.items() if pp == p and v == 'alert'):,}  "
                  f"past the limit ${past[p]:,.4f} ({100 * past[p] / used if used else 0:.3f}% of usage)", flush=True)
            stats.clear()
            old = p - 2                     # forget periods that ended more than one period ago
            for d in (total, state):
                for k in [k for k in d if k[0] <= old]:
                    del d[k]
            for k in [k for k in last if k[0] <= old]:
                del last[k]

    parts = list(range((await (await own_client()).get_topic(STREAM, USAGE)).partitions_count))
    await asyncio.gather(send_decisions(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the usage partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="limits")
    p.add_argument("--budget-ms", type=int, default=50, help="usage-to-limit-check p99 budget for the lane")
    p.add_argument("--users", type=int, default=20_000, help="must match api_servers.py")
    p.add_argument("--requests-per-s", type=float, default=10_000, help="must match api_servers.py (sets the limits)")
    p.add_argument("--period-s", type=float, default=60, help="seconds per compressed billing period; must match api_servers.py")
    p.add_argument("--alert-at", type=float, default=0.8, help="send an alert at this fraction of the limit")
    asyncio.run(main(p.parse_args()))
