"""Routing: CDN health signals applied to where new plays go, and the cost of a late signal counted.

Reads `cdn-health` (the lane out) on its own connection and consumer group and keeps the current
state of every cohort|cdn pair, ignoring any older or repeated sequence. It stands in for the readers
of that signal: the QoS Routing Manager that sets CDN weights per cohort, and real-time alerting
(run a second copy with --name alerting). Your routing manager keeps its own guards (hard ASN
constraints, a cap on change per minute, a warm-up floor); this script applies a flag at once, so
what it counts is the signal's arrival and nothing else.

It also plays new plays starting, --plays-per-s of them, each in one cohort, assigned a CDN by
the usual shares. A CDN flagged degraded for that cohort is skipped. The business number: plays
still sent to a CDN after the health check had flagged it for that cohort, because the signal had
not arrived yet. That count belongs to the lane alone. For every incident it also prints how long
the pair was degraded before the flag (the detection window, set by the scoring and the heartbeat
rate) and how long the signal took from the health check to routing (the lane).

Prints check-to-routing p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import itertools
import os
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import CDNS, DEFAULT_URL, SIGNALS, STREAM, Window, cohorts, ensure_topics, load


async def main(a):
    client = IggyClient.from_connection_string(a.url)   # this reader's own connection
    await client.connect()
    await ensure_topics(client, SIGNALS)
    signals = await client.consumer_group(
        f"{a.name}-{os.getpid()}", STREAM, SIGNALS, polling_strategy=PollingStrategy.First(), batch_length=1000,
        auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))
    table = cohorts()
    names, cum = [c for c, _ in table], list(itertools.accumulate(w for _, w in table))
    cdns, cdn_cum = [c for c, _ in CDNS], list(itertools.accumulate(w for _, w in CDNS))
    view = {}                               # pair -> (seq, status)
    assigned = defaultdict(deque)           # pair -> when a play was last sent there (ns), past few seconds
    lat, stats, total = Window(), defaultdict(int), defaultdict(int)
    caught_up = [False]                     # a new reader first replays earlier signals
    gc.freeze()

    async def on_signal(m):
        s = load(m.payload())
        k = s["k"]
        before = view.get(k, (0, "healthy"))
        if s["seq"] <= before[0]:
            stats["dropped"] += 1           # older or repeated signal: the newest state wins
            return
        view[k] = (s["seq"], s["status"])
        now = time.time_ns()
        age = (now - s["ns"]) / 1e6
        if not caught_up[0]:                # still replaying: track the oldest signal seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
            return
        lat.add(age)
        if s["status"] == "degraded" and before[1] != "degraded":
            late = sum(1 for t in assigned[k] if t >= s["ns"])   # sent there after the flag, before the signal arrived
            total["incidents"] += 1
            total["late"] += late
            stats["late"] += late
            detect = (s["ns"] - s["onset"]) / 1e9 if s["onset"] else float("nan")
            print(f"[{a.name}] degraded {k}  {s['sev']}  degraded {detect:.2f} s before the flag (detection window)  "
                  f"flag to {a.name} {age:.1f} ms (lane)  plays sent after the flag {late}", flush=True)
        elif s["status"] == "healthy" and before[1] == "degraded":
            total["healthy"] += 1
            print(f"[{a.name}] healthy  {k}  back in rotation", flush=True)

    async def plays():
        due, owed = time.monotonic(), 0.0
        while True:
            owed += a.plays_per_s / 100
            now = time.time_ns()
            for c in random.choices(names, cum_weights=cum, k=int(owed)):
                cdn = random.choices(cdns, cum_weights=cdn_cum)[0]
                if view.get(f"{c}|{cdn}", (0, "healthy"))[1] == "degraded":
                    ok = [d for d in cdns if view.get(f"{c}|{d}", (0, "healthy"))[1] != "degraded"]
                    if ok:
                        cdn = random.choice(ok)
                        stats["steered"] += 1   # sent to a healthy CDN for this cohort instead
                assigned[f"{c}|{cdn}"].append(now)
                stats["plays"] += 1
            owed -= int(owed)
            for q in assigned.values():
                while q and q[0] < now - 3_000_000_000:
                    q.popleft()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of signals arrived inside the budget; partitions interleave,
                # so one fresh signal alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier signals this second" if stats["replayed"]
                      else f"[{a.name}] waiting for the first health signals", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            down = sum(1 for v in view.values() if v[1] == "degraded")
            print(f"[{a.name}] {r['n']:>5,} signals/s  check-to-{a.name} p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  pairs degraded {down}", flush=True)
            print(f"[{a.name}] plays {stats['plays']:,}/s  steered to a healthy CDN {stats['steered']:,}  "
                  f"sent after the flag {stats['late']}  (so far: incidents {total['incidents']}, "
                  f"healthy again {total['healthy']}, plays sent after the flag {total['late']})", flush=True)
            stats.clear()

    await asyncio.gather(signals.consume_messages(on_signal, asyncio.Event()), plays(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="routing")
    p.add_argument("--plays-per-s", type=float, default=2_000, help="new plays starting")
    p.add_argument("--budget-ms", type=int, default=100, help="check-to-routing p99 budget for the lane")
    asyncio.run(main(p.parse_args()))
