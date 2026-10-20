"""The latest-state service the app reads, fed by the vehicle state lane.

Reads app-vehicle-state on its own connection and consumer group and keeps, for every vehicle, the newest
value of every signal the app shows (a stand-in for the latest-state cache behind your telematics API).
Each report carries the vehicle's sequence and resulting values, so a replayed or repeated report is
recognised and changes nothing, and a restarted service rebuilds every vehicle's state from the retained
topic: it holds nothing that the log does not.

It also plays the app: --reads-per-s app opens a second, half of them for vehicles whose lock or charging
state just changed (the owner who just walked away or plugged in checks the app), half for any vehicle.
Each served state goes to app-reads in one batch every 10 ms, from its own task, never from the read loop,
so vehicles.py can check it against the truth.

Prints report-to-state p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy, SendMessage

from common import DEFAULT_URL, PARTITIONS, READS, STATE, STREAM, Stalls, Window, ensure_topics, load, msg


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), STATE, READS)
    reports = await (await own_client()).consumer_group(
        f"{a.name}-{os.getpid()}", STREAM, STATE, polling_strategy=PollingStrategy.First(),   # rebuild from the log
        batch_length=5000, auto_commit=AutoCommit.Interval(timedelta(seconds=1)),           # offsets once a second
        poll_interval=timedelta(microseconds=500))
    out = await (await own_client()).producer(STREAM, READS, topic_partitions_count=PARTITIONS[READS])
    state = {}                              # vehicle -> newest report applied (the app's view)
    keys = []                               # every vehicle seen, for app opens on any vehicle
    changed = deque(maxlen=2000)            # vehicles whose lock or charging state just changed
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    caught_up = [False]                     # a new reader first replays the reports retained on the topic
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def on_report(m):
        d = load(m.payload())
        age = (time.time_ns() - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest report seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = d["v"]
        old = state.get(k)
        if old is None:
            keys.append(k)
        elif d["seq"] <= old["seq"]:
            stats["dropped"] += 1           # older or repeated report: already applied
            return
        state[k] = d
        stats["reports"] += 1
        if old is not None and (old.get("locked"), old.get("charge")) != (d.get("locked"), d.get("charge")):
            stats["changes"] += 1
            if caught_up[0]:
                changed.append(k)

    async def app_opens():
        # One batch of app opens every 10 ms, sized to what the tick owes, sent from this task only.
        r, owed, last = random.Random(11), 0.0, time.monotonic()
        async with out:
            while True:
                await asyncio.sleep(0.01)
                now = time.monotonic()
                owed += a.reads_per_s * (now - last)
                last, n = now, int(owed)
                owed -= n
                if not n or not caught_up[0] or not keys:
                    continue
                batch = []
                for i in range(n):
                    chg = i % 2 == 0 and bool(changed)
                    k = r.choice(changed) if chg else r.choice(keys)
                    s = state[k]
                    batch.append(SendMessage(msg(v=k, seq=s["seq"], locked=s.get("locked"), charge=s.get("charge"),
                                                 soc=s.get("soc"), chg=int(chg))))
                await out.send(batch)
                stats["served"] += len(batch)

    async def report_line():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of reports arrived inside the budget; partitions interleave,
                # so one fresh report alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier reports this second, "
                      f"state rebuilt for {len(state):,} vehicles", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} reports/s  report-to-state p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  lock or charge changes "
                  f"{int(stats['changes']):,}  app reads served {int(stats['served']):,}  vehicles {len(state):,}  "
                  f"{stalls.read()}", flush=True)
            stats.clear()

    await asyncio.gather(reports.consume_messages(on_report, asyncio.Event()), app_opens(), report_line(),
                         stalls.watch())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="vehicle-state")
    p.add_argument("--budget-ms", type=int, default=50, help="report-to-state p99 budget for the lane")
    p.add_argument("--reads-per-s", type=float, default=2000, help="app opens a second")
    asyncio.run(main(p.parse_args()))
