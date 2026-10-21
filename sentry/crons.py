"""The source of truth: customers' cron jobs checking in through Relay, as the Crons check-in lane sees them.

Every monitor runs on its schedule (every 1, 5, 15 or 60 minutes by default). At each run the job sends an
in_progress check-in a moment after the minute starts (cron jobs start on the minute, so check-ins arrive in
a burst), then ok or error when it finishes. Check-ins are keyed by monitor, so one monitor's check-ins stay
in order on one partition. At every minute boundary a clock pulse goes into every partition, as Sentry's
clock_pulse task does, so the slowest partition always has something to move its clock.

Two things happen on purpose, so the clock has something to get right:
  * jobs that never check in: --never-rate of runs send nothing at all. The monitor consumer should mark
    each one missed at the first tick past its due minute plus the check-in margin.
  * jobs that hang: --hang-rate of runs send in_progress and then nothing. Each should be marked timed out
    at the first tick past its start plus max_runtime.

crons.py also reads every verdict the monitor consumer sends (monitor-verdicts) and checks it against the
truth: was every missed mark a run that really never checked in, was every timeout a job that really hung,
how long after the deadline did each mark arrive, and did any deadline pass by --grace-s with no mark?
--minute-s sets the length of a minute: 60 is real time; CI runs 5.
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (CHECKINS, DEFAULT_URL, PARTITIONS, STREAM, VERDICTS, Stalls, Window, ensure_topics, load,
                    monitor, msg, part)

TICK = 0.01   # seconds between send rounds; each check-in is stamped when it is sent


def schedule(k: int) -> str:
    return "* * * * *" if k == 1 else "0 * * * *" if k == 60 else f"*/{k} * * * *"


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), CHECKINS, VERDICTS)
    cfg = DirectProducerConfig(batch_length=5000, linger_time=timedelta(microseconds=500))
    out = await (await own_client()).producer(STREAM, CHECKINS, topic_partitions_count=PARTITIONS[CHECKINS], mode=cfg)
    verdicts = await (await own_client()).consumer_group(
        "crons-truth-check", STREAM, VERDICTS, polling_strategy=PollingStrategy.Next(), batch_length=5000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)),     # offsets stored once a second, not per poll
        poll_interval=timedelta(milliseconds=5))                    # a check, not on the lane's clock

    M, P, R, margin = a.minute_s, PARTITIONS[CHECKINS], a.max_runtime, a.margin
    r = random.Random(7)
    mix = [(int(k), float(w)) for k, w in (x.split(":") for x in a.mix.split(","))]
    names = [monitor(i) for i in range(a.monitors)]
    index = {n: i for i, n in enumerate(names)}
    every = [r.choices([k for k, _ in mix], [w for _, w in mix])[0] for _ in range(a.monitors)]
    pid = [part(n, P) for n in names]
    groups = defaultdict(list)              # k -> monitors that run every k minutes
    for i, k in enumerate(every):
        groups[k].append(i)
    conf = [{"schedule": schedule(k), "checkin_margin": margin, "max_runtime": R} for k in every]
    started = [False] * a.monitors          # a monitor exists once it has checked in once
    kinds = {}                              # (monitor, due minute) -> "never" | "hang" | "ok"
    by_minute = defaultdict(list)           # due minute -> keys, to forget old runs
    wait_missed, wait_timeout = {}, {}      # (monitor, due) -> deadline (s) still waiting for its mark
    heap, seq = [], [0]                     # (send time, n, partition, message)
    stats, delays, stalls = defaultdict(int), Window(), Stalls()
    gc.freeze()

    def push(t, p, m):
        seq[0] += 1
        heapq.heappush(heap, (t, seq[0], p, m))

    def plan(D):
        """Plan minute D: a clock pulse into every partition at the boundary, and every run due at D."""
        for p in range(P):
            push(D * M, p, ("pulse",))
        for k, mons in groups.items():
            if D % k:
                continue
            for i in mons:
                key = (i, D)
                x = r.random()
                kind = ("ok" if not started[i] else "never" if x < a.never_rate
                        else "hang" if x < a.never_rate + a.hang_rate else "ok")
                started[i] = True
                kinds[key] = kind
                by_minute[D].append(key)
                stats["runs"] += 1
                if kind == "never":
                    wait_missed[key] = (D + margin) * M
                    continue
                start = D * M + min(0.6, 0.02 + r.expovariate(1 / a.start_spread)) * M
                cid = f"{i}-{D}"
                push(start, pid[i], ("in_progress", i, cid))
                if kind == "hang":
                    wait_timeout[key] = (D + R) * M
                else:
                    push(start + r.uniform(0.1, R - 1.1) * M, pid[i],
                         ("error" if r.random() < a.error_rate else "ok", i, cid))

    def encode(m, p):
        if m[0] == "pulse":
            return SendMessage(msg(type="clock_pulse", partition_ids=list(range(P))))
        status, i, cid = m
        return SendMessage(msg(type="check_in", monitor_slug=names[i], status=status, check_in_id=cid,
                               monitor_config=conf[i]))

    async def run():
        async with out:
            planned = int(time.time() // M)  # the current minute is not planned: monitors start next minute
            plan(planned + 1)
            planned += 1
            while True:
                now = time.time()
                if int(now // M) + 1 > planned:  # plan a minute ahead, so the pulse goes out on the boundary
                    planned += 1
                    plan(planned)
                    for D in [d for d in by_minute if d < planned - R - 4 - a.grace_s / M]:
                        for key in by_minute.pop(D):
                            kinds.pop(key, None)
                batches = defaultdict(list)
                while heap and heap[0][0] <= now:
                    _, _, p, m = heapq.heappop(heap)
                    batches[p].append(encode(m, p))
                    stats["checkins" if m[0] != "pulse" else "pulses"] += 1
                if batches:
                    # One batch per partition, all sent at once: a monitor's check-ins keep their order.
                    await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                nxt = min(heap[0][0] if heap else now + TICK, now + TICK)
                await asyncio.sleep(max(0.0, nxt - time.time()))

    async def on_verdict(m):
        d = load(m.payload())
        i = index.get(d["slug"])
        if i is None:
            return
        if d["v"] == "missed":
            key = (i, d["due"])
            if kinds.get(key) == "never":
                if key in wait_missed:
                    stats["missed_right"] += 1
                    delays.add((d["ns"] / 1e9 - wait_missed.pop(key)) * 1000)
                    stats["by_next"] += d.get("how") == "next check-in"
                else:
                    stats["dup"] += 1
            elif key in kinds:
                stats["missed_false"] += 1  # a run that did check in was marked missed
                print(f"[crons] false missed: {d['slug']} run due minute {d['due']} ({kinds[key]}), "
                      f"marked at tick {d['tick']} by {d.get('how')}", flush=True)
        elif d["v"] == "timeout":
            key = (i, int(d["id"].split("-")[1]))
            if kinds.get(key) == "hang" and key in wait_timeout:
                stats["timeout_right"] += 1
                delays.add((d["ns"] / 1e9 - wait_timeout.pop(key)) * 1000)
            elif key in kinds:
                stats["timeout_false"] += 1  # a job that finished was marked timed out
                print(f"[crons] false timeout: {d['slug']} run {d['id']} ({kinds[key]}) at tick {d['tick']}",
                      flush=True)

    async def report_line():
        while True:
            await asyncio.sleep(1)
            now = time.time()
            for w, k in ((wait_missed, "missed_undetected"), (wait_timeout, "timeout_undetected")):
                for key, deadline in list(w.items()):
                    if now - deadline > a.grace_s:
                        stats[k] += 1       # the deadline passed and no mark arrived in time
                        del w[key]
            d = delays.flush()
            print(f"[crons] {stats['checkins']:>6,} check-ins/s  monitors {a.monitors:,} (minute = {M:g} s)  "
                  f"runs due {stats['runs']:,}  missed: right {stats['missed_right']:,} (by next check-in "
                  f"{stats['by_next']:,})  false {stats['missed_false']:,}  undetected {stats['missed_undetected']:,}  "
                  f"timeouts: right {stats['timeout_right']:,}  false {stats['timeout_false']:,}  "
                  f"undetected {stats['timeout_undetected']:,}  mark after deadline p50 {d['p50']:,.0f}ms  "
                  f"max {d['max']:,.0f}ms  {stalls.read()}", flush=True)
            stats.clear()

    await asyncio.gather(run(), verdicts.consume_messages(on_verdict, asyncio.Event()), report_line(),
                         stalls.watch())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--monitors", type=int, default=50000, help="cron monitors checking in")
    ap.add_argument("--mix", default="1:0.1,5:0.3,15:0.3,60:0.3",
                    help="minutes between runs and share of monitors, e.g. 1:0.1,5:0.3,15:0.3,60:0.3")
    ap.add_argument("--minute-s", type=float, default=60, help="seconds in a clock minute (CI compresses it)")
    ap.add_argument("--margin", type=int, default=1, help="check-in margin in minutes")
    ap.add_argument("--max-runtime", type=int, default=3, help="max runtime in minutes before a job times out")
    ap.add_argument("--start-spread", type=float, default=0.08,
                    help="mean delay of in_progress after the minute starts, as a share of a minute")
    ap.add_argument("--never-rate", type=float, default=0.02, help="share of runs whose job never checks in")
    ap.add_argument("--hang-rate", type=float, default=0.01, help="share of runs that hang after in_progress")
    ap.add_argument("--error-rate", type=float, default=0.02, help="share of runs that finish with error")
    ap.add_argument("--grace-s", type=float, default=5, help="a deadline passed this long with no mark is undetected")
    asyncio.run(main(ap.parse_args()))
