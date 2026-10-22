"""The workflow engine: NF health alerts applied to where new procedures go, and the cost of a late alert counted.

Reads `nf-health` (the lane out) directly, partition by partition, on its own connection, and keeps the
current state of every NF instance, ignoring any older or repeated sequence. It stands in for the readers
of that alert: ATOM's workflow engine, which Jio describes as orchestrating operational tasks between
systems, and the NOC view (run a second copy with --name noc). Your workflow keeps its own guards (a cap
on how many instances a pool may drain, a hold-down before taking one back); this script applies an alert
at once, so what it counts is the alert's arrival and nothing else.

It also plays new control-plane procedures starting, --procedures-per-s of them, each in one pool (one NF
type in one circle) and given to one of the pool's instances. An instance flagged degraded is skipped and
a healthy peer takes the procedure: the whitepaper's Data Management Services layer keeps session data
outside the CNF, so another CNF with instant access to the same data can carry on. The business number: procedures still given to
an instance after the anomaly check had flagged it, because the alert had not arrived yet. That count
belongs to the lane alone. For every incident it also prints how long the instance was degraded before the
flag (the detection window, set by the models and the record rate) and how long the alert took from the
anomaly check to the workflow engine (the lane).

Prints check-to-workflow p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
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

from common import ALERTS, DEFAULT_URL, INSTANCES, STREAM, Window, ensure_topics, load, pools, read_partitions


async def main(a):
    client = IggyClient.from_connection_string(a.url)   # this reader's own connection
    await client.connect()
    await ensure_topics(client, ALERTS)
    table = pools()
    names, cum = [c for c, _ in table], list(itertools.accumulate(w for _, w in table))
    view = {}                               # NF instance -> (seq, status)
    assigned = defaultdict(deque)           # NF instance -> when a procedure was last given to it (ns), past few s
    lat, stats, total = Window(), defaultdict(int), defaultdict(int)
    caught_up = [False]                     # a new reader first replays earlier alerts
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_alert(m):
        s = load(m.payload())
        k = s["k"]
        before = view.get(k, (0, "healthy"))
        if s["seq"] <= before[0]:
            stats["dropped"] += 1           # older or repeated alert: the newest state wins
            return
        view[k] = (s["seq"], s["status"])
        now = time.time_ns()
        age = (now - s["ns"]) / 1e6
        if not caught_up[0]:                # still replaying: track the oldest alert seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
            return
        lat.add(age)
        if s["status"] == "degraded" and before[1] != "degraded":
            late = sum(1 for t in assigned[k] if t >= s["ns"])   # given to it after the flag, before the alert arrived
            total["incidents"] += 1
            total["late"] += late
            stats["late"] += late
            detect = (s["ns"] - s["onset"]) / 1e9 if s["onset"] else float("nan")
            print(f"[{a.name}] degraded {k}  {s['sev']}  degraded {detect:.2f} s before the flag (detection window)  "
                  f"flag to {a.name} {age:.1f} ms (lane)  procedures given after the flag {late}", flush=True)
        elif s["status"] == "healthy" and before[1] == "degraded":
            total["healthy"] += 1
            print(f"[{a.name}] healthy  {k}  back in its pool", flush=True)

    async def procedures():
        due, owed = time.monotonic(), 0.0
        while True:
            owed += a.procedures_per_s / 100
            now = time.time_ns()
            for pool in random.choices(names, cum_weights=cum, k=int(owed)):
                members = [f"{pool}-{i:02d}" for i in range(1, INSTANCES + 1)]
                nf = random.choice(members)
                if view.get(nf, (0, "healthy"))[1] == "degraded":
                    ok = [m for m in members if view.get(m, (0, "healthy"))[1] != "degraded"]
                    if ok:
                        nf = random.choice(ok)
                        stats["steered"] += 1   # a healthy peer in the same pool takes it instead
                assigned[nf].append(now)
                stats["procedures"] += 1
            owed -= int(owed)
            for q in assigned.values():
                while q and q[0] < now - 3_000_000_000:
                    q.popleft()
            due += 0.01                     # from the due time, so a slow tick doesn't drift the schedule
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of alerts arrived inside the budget; partitions interleave,
                # so one fresh alert alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier alerts this second"
                      if stats["replayed"] else f"[{a.name}] waiting for the first NF health alerts", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            down = sum(1 for v in view.values() if v[1] == "degraded")
            print(f"[{a.name}] {r['n']:>5,} alerts/s  check-to-{a.name} p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  NFs degraded {down}", flush=True)
            print(f"[{a.name}] procedures {stats['procedures']:,}/s  steered to a healthy peer {stats['steered']:,}  "
                  f"given after the flag {stats['late']}  (so far: incidents {total['incidents']}, "
                  f"healthy again {total['healthy']}, procedures given after the flag {total['late']})", flush=True)
            stats.clear()

    parts = list(range((await client.get_topic(STREAM, ALERTS)).partitions_count))
    reader = IggyClient.from_connection_string(a.url)
    await reader.connect()
    await asyncio.gather(read_partitions(reader, ALERTS, parts, f"{a.name}-{os.getpid()}", on_alert),
                         procedures(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="workflow")
    p.add_argument("--procedures-per-s", type=float, default=5_000, help="new control-plane procedures starting")
    p.add_argument("--budget-ms", type=int, default=100, help="check-to-workflow p99 budget for the lane")
    asyncio.run(main(p.parse_args()))
