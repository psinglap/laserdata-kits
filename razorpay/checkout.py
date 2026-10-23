"""Checkout: downtime signals applied to the payment methods it shows, and the cost of a late signal counted.

Reads `downtime-signals` (the lane out) on its own connection, every partition read directly, and keeps the
current state of every cohort, ignoring any older or repeated sequence. Razorpay's Checkout uses
these signals to warn customers and steer them to another instrument; merchants get the same
signal through the Downtime API and webhooks (run a second copy with --name webhooks).

It also plays customers opening Checkout, --checkouts-per-s of them, each about to pay with one
cohort. A cohort already flagged down is hidden. The business number: checkouts that still
offered a cohort after the detector had flagged it, because the signal had not arrived yet. That
count belongs to the lane alone. For every downtime it also prints how long the cohort was
degraded before the detector flagged it (the detection window, set by the rule and the payment
rate) and how long the signal took from the detector to Checkout (the lane).

Prints detector-to-checkout p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import itertools
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import IggyClient

from common import DEFAULT_URL, PARTITIONS, SIGNALS, Window, cohorts, ensure_topics, load, read_partitions


async def main(a):
    client = IggyClient.from_connection_string(a.url)   # this reader's own connection
    await client.connect()
    await ensure_topics(client, SIGNALS)
    table = cohorts()
    keys, cum = [k for k, _, _ in table], list(itertools.accumulate(w for _, w, _ in table))
    view = {}                               # cohort -> (seq, status)
    offered = defaultdict(deque)            # cohort -> when Checkout last offered it (ns), past few seconds
    lat, stats, total = Window(), defaultdict(int), defaultdict(int)
    caught_up = [False]                     # a new reader first replays earlier signals
    gc.freeze()

    async def on_signal(m):
        s = load(m.payload())
        k = s["k"]
        before = view.get(k, (0, "up"))
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
        if s["status"] == "started" and before[1] != "started":
            late = sum(1 for t in offered[k] if t >= s["ns"])   # offered after the flag, before the signal arrived
            total["downtimes"] += 1
            total["late"] += late
            stats["late"] += late
            detect = (s["ns"] - s["onset"]) / 1e9 if s["onset"] else float("nan")
            print(f"[{a.name}] downtime started {k}  degraded {detect:.2f} s before the flag (detection window)  "
                  f"flag to {a.name} {age:.1f} ms (lane)  offered after the flag {late}", flush=True)
        elif s["status"] == "resolved" and before[1] == "started":
            total["resolved"] += 1
            print(f"[{a.name}] downtime resolved {k}  shown again", flush=True)

    async def customers():
        due, owed = time.monotonic(), 0.0
        while True:
            owed += a.checkouts_per_s / 100
            now = time.time_ns()
            for k in random.choices(keys, cum_weights=cum, k=int(owed)):
                if view.get(k, (0, "up"))[1] == "started":
                    stats["hidden"] += 1    # steered to another method before paying
                else:
                    offered[k].append(now)
                stats["checkouts"] += 1
            owed -= int(owed)
            for q in offered.values():
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
                      else f"[{a.name}] waiting for the first downtime signals", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            down = sum(1 for v in view.values() if v[1] == "started")
            print(f"[{a.name}] {r['n']:>5,} signals/s  detector-to-{a.name} p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  cohorts down {down}", flush=True)
            print(f"[{a.name}] checkouts {stats['checkouts']:,}/s  method hidden {stats['hidden']:,}  "
                  f"offered after the flag {stats['late']}  (so far: downtimes {total['downtimes']}, "
                  f"resolved {total['resolved']}, offered after the flag {total['late']})", flush=True)
            stats.clear()

    await asyncio.gather(read_partitions(client, SIGNALS, range(PARTITIONS[SIGNALS]), f"{a.name}-{os.getpid()}", on_signal,
                                         replay=True, batch=1000, poll_ms=0.5), customers(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="checkout")
    p.add_argument("--checkouts-per-s", type=float, default=2_000, help="customers opening Checkout")
    p.add_argument("--budget-ms", type=int, default=100, help="detector-to-checkout p99 budget for the lane")
    asyncio.run(main(p.parse_args()))
