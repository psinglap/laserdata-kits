"""An ad server with its budget view: it inserts ads only for ad groups with budget left, and shares every
insertion's in-flight spend with every other ad server through the in-flight spend lane.

Each process is one ad server; run several (ci.sh runs three) with different --name and the same settings.
--rate ad requests a second. Each request is an auction with up to --tries candidate ad groups, drawn so that
a small share of ad groups wins a large share of auctions. A candidate is inserted only if Pinterest's rule,
checked with this insertion's own cost, says there is room:

    actual_spend + inflight_spend + this_insertion <= daily_budget

so any spend past a budget can only come from another server's insertion that has not reached this one yet.
In the kit, every insertion's in-flight spend stays in the day's total (as if each is charged at its
expected cost); in production, the spend system's actual spend replaces it as charges land.

Every insertion goes on inflight-spend, keyed by ad group: the server's own running in-flight spend for that
ad group in the day (the resulting total, never a delta), with a per-(server, ad group) sequence; the server
and sequence are the event ID. With --dup-rate, a share of insertions is delivered twice, as an
at-least-once pipeline does; readers drop the repeat by its event ID, so no spend is counted twice.

The server reads every partition of the lane directly (its own offsets, no consumer group) on its own
connection and applies the other servers' spend to its budget view. Its own spend at once. On start it
rebuilds its view from the log, from the start of the previous budget day, before it inserts any ad.

Prints a line a second: spend-to-budget-view latency (published on another server -> applied here), PASS or
FAIL against --budget-ms, candidates skipped for lack of budget, unfilled requests, repeats dropped, sequence
gaps. When a budget day closes, the business numbers: spend, ad groups that reached their budget, and
overdelivery: spend past budget (free ads), because an insertion elsewhere was still on its way.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, PARTITIONS, SPEND, STREAM, Window, adgroup, budget_day, connect, daily_budget,
                    ensure_topics, inflight_cost, load, msg, part)


async def main(a):
    me = f"{a.name}.{os.getpid()}"          # a restarted server is a new writer, so its totals start again
    await ensure_topics(await connect(a.url), SPEND)
    writer, reader = await connect(a.url), await connect(a.url)     # one connection each way
    P = PARTITIONS[SPEND]
    ags = [adgroup(i) for i in range(a.adgroups)]
    agpart = [part(g, P) for g in ags]
    cost = [inflight_cost(i) for i in range(a.adgroups)]
    limit = {ags[i]: daily_budget(i, a.budget_min, a.budget_max) for i in range(a.adgroups)}
    cost_of = {ags[i]: cost[i] for i in range(a.adgroups)}
    rng = random.Random(me)
    start_ns = time.time_ns()

    # Per budget day, so a closed day is forgotten in one step, not by scanning every key.
    total = defaultdict(dict)               # day -> {ad group: in-flight spend, all ad servers, micro-dollars}
    last_n = defaultdict(dict)              # day -> {(writer, ad group): that writer's latest total}
    last_seq = {}                           # (writer, ad group) -> last sequence applied
    my_seq = defaultdict(int)               # ad group -> this server's sequence
    my_n = defaultdict(dict)                # day -> {ad group: this server's in-flight spend}
    spent, over, free, at_budget = (defaultdict(int) for _ in range(4))   # per day
    lat, st, run = Window(), defaultdict(int), defaultdict(int)  # per-second / whole-run counters
    live, caught, held = set(), [None], deque()
    gc.freeze()                             # keep start-up tables out of the collector's way

    def apply(e):
        k = (e["s"], e["g"])
        prev = last_seq.get(k)
        if prev is not None and e["seq"] <= prev:
            run["repeats"] += 1             # same event ID seen before (retry, redelivery, replay): count it once
            return
        if prev is not None and e["seq"] > prev + 1:
            run["gaps"] += 1                # an insertion from this server never arrived
        last_seq[k] = e["seq"]
        d_, g = e["d"], e["g"]
        ln, tw = last_n[d_], total[d_]
        nk = (e["s"], g)
        d = e["n"] - ln.get(nk, 0)
        if d <= 0:
            return
        ln[nk] = e["n"]
        before, b = tw.get(g, 0), limit.get(g, 0)
        tw[g] = before + d
        spent[d_] += d
        c = cost_of.get(g, d)
        if before + c <= b < before + d + c:
            at_budget[d_] += 1              # no room left for another insertion of this ad group today
        if before + d > b:
            over[d_] += before + d - max(before, b)     # only spend past the budget counts: free ads
            free[d_] += 1

    def release(now):
        # --hold-ms: apply other servers' spend only once it is this old (a slower pipeline)
        while held and now - held[0]["ns"] >= a.hold_ms * 1_000_000:
            e = held.popleft()
            lat.add((now - e["ns"]) / 1e6)
            apply(e)

    async def read():
        who = {p: Consumer.Single(f"{me}-{p}") for p in range(P)}
        since = PollingStrategy.Timestamp(int((budget_day(a.day_s) - 1) * a.day_s * 1e6))
        nxt = {}
        while True:
            more = False
            for p in range(P):
                ms = await reader.poll_messages(STREAM, SPEND, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        e = load(m.payload())
                        if e["s"] == me:
                            continue        # this server's own spend was applied when it inserted
                        if caught[0] is None or e["ns"] < start_ns:
                            run["replayed"] += 1    # rebuilding: not live latency
                            apply(e)
                        elif a.hold_ms:
                            held.append(e)
                        else:
                            lat.add((now - e["ns"]) / 1e6)
                            apply(e)
                if len(ms) == a.batch:
                    more = True             # a replay or a burst: read on at once
                elif p not in live:
                    live.add(p)             # this partition has been read to the end of the log once
                    if len(live) == P:
                        caught[0] = time.monotonic()
                        seen = len({g for tw in total.values() for g in tw})
                        print(f"[{a.name}] rebuilt the budget view from the log in {(time.time_ns() - start_ns) / 1e9:.2f}s: "
                              f"{seen:,} ad groups, {run['replayed']:,} insertions since the previous day began",
                              flush=True)
            if held:
                release(time.time_ns())
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def serve():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never insert an ad before the budget view is rebuilt
        due = time.monotonic()
        owed = 0.0
        while True:
            d_ = budget_day(a.day_s)
            tw, mine = total[d_], my_n[d_]
            batches = defaultdict(list)
            owed += a.rate * a.tick_ms / 1000
            for _ in range(int(owed)):
                for _ in range(a.tries):    # the auction's candidates, best first
                    i = int(a.adgroups * rng.random() ** a.skew)
                    g = ags[i]
                    if tw.get(g, 0) + cost[i] <= limit[g]:
                        break
                    st["no_budget"] += 1    # the budget at work: this ad group sits out the auction
                else:
                    st["unfilled"] += 1     # every candidate was out of budget
                    continue
                my_seq[g] += 1
                n = mine[g] = mine.get(g, 0) + cost[i]
                e = {"s": me, "g": g, "d": d_, "seq": my_seq[g], "n": n, "ns": time.time_ns()}
                apply(e)                    # this server knows its own spend at once
                m = msg(**e)
                batches[agpart[i]].append(SendMessage(m))
                if a.dup_rate and rng.random() < a.dup_rate:
                    batches[agpart[i]].append(SendMessage(m))    # delivered twice, same event ID
                    st["dups"] += 1
                st["inserted"] += 1
            owed -= int(owed)
            # One batch per partition, all sent at once: an ad group's spend stays in order, one round trip a tick.
            if batches:
                await asyncio.gather(*(writer.send_messages(STREAM, SPEND, Partitioning.partition_id(p), ms)
                                       for p, ms in batches.items()))
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        closed = budget_day(a.day_s) - 1    # days summarised so far (none before start)
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the budget view from the log: {run['replayed']:,} insertions so far", flush=True)
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {st['inserted']:>5,} insertions/s  from other servers {r['n']:>6,}/s  "
                  f"spend-to-budget-view p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  "
                  f"{ok} (<{a.budget_ms:g}ms)  out of budget {st['no_budget']:,}  unfilled {st['unfilled']:,}  "
                  f"repeats dropped {run['repeats']:,}  gaps {run['gaps']}", flush=True)
            st.clear()
            d_ = budget_day(a.day_s)
            if d_ - 1 > closed and time.time() - d_ * a.day_s >= 1.0 + a.hold_ms / 1000:   # its tally is in by now
                c = closed = d_ - 1
                s, o = spent[c], over[c]
                print(f"[day] {c % 10000} closed: spend ${s / 1e6:,.2f}  ad groups at budget {at_budget[c]:,}  "
                      f"overdelivery ${o / 1e6:,.2f} ({100 * o / s if s else 0:.3f}% of spend, "
                      f"{free[c]:,} insertions past budget)", flush=True)
                for d in (total, last_n, my_n):     # forget days that ended more than one day ago
                    for k in [k for k in d if k < c]:
                        del d[k]

    await asyncio.gather(read(), serve(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="ads-1")
    p.add_argument("--rate", type=float, default=2_000, help="ad requests (auctions) this server runs a second")
    p.add_argument("--adgroups", type=int, default=20_000, help="ad groups in play; same on every server")
    p.add_argument("--skew", type=float, default=2.0, help="higher: a smaller share of ad groups wins most auctions")
    p.add_argument("--tries", type=int, default=4, help="candidate ad groups per auction")
    p.add_argument("--budget-min", type=int, default=5, help="smallest daily budget, in insertions of that ad group")
    p.add_argument("--budget-max", type=int, default=100, help="largest daily budget, in insertions of that ad group")
    p.add_argument("--day-s", type=float, default=60, help="budget day in seconds (a compressed day); same everywhere")
    p.add_argument("--budget-ms", type=float, default=50, help="spend-to-budget-view p99 budget")
    p.add_argument("--dup-rate", type=float, default=0.005, help="share of insertions delivered twice")
    p.add_argument("--hold-ms", type=float, default=0,
                   help="what-if: apply other servers' spend only after this delay, like a slower spend pipeline")
    p.add_argument("--tick-ms", type=float, default=10, help="how often the server sends its insertions' spend")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
