"""The monitor consumer and its clock, fed by the Crons check-in lane.

Reads every partition of ingest-monitors directly, on --readers connections of its own, from the start of
the retained log and then from its own position. For every monitor it keeps the next expected run, and for
every job that sent in_progress, when it times out. The clock works the way Sentry Crons describes it:

  * each partition's clock is the log time (the broker's append time) of the newest message read from it,
    clamped to the minute; a clock pulse sent into every partition at each minute keeps quiet partitions
    moving;
  * the clock ticks a minute only when every partition has passed it (the slowest partition sets the time),
    and it holds while any partition has not reported at all;
  * if the clock jumps more than a minute, every skipped minute is ticked in order (a backfill).

At each tick T:
  missed     a monitor whose next run plus its check-in margin is due by T, and no in_progress came: the job
             never checked in. Mark that run missed and expect the next one.
  timed out  a job that sent in_progress at minute S and nothing since, with S + max_runtime due by T.

A check-in for a later run also marks any run it skipped as missed, so the answer does not depend on which
partition the clock was waiting for. Each mark goes to monitor-verdicts in one batch every 10 ms, from its own
task, never from the read loop, so crons.py can check it against the truth.

--minute-s sets the length of a clock minute: 60 is real time; CI runs 5 so a one-minute run sees a dozen
ticks. --pause-partition stops reading one partition for --pause-for seconds (a stuck consumer, or a
backlog on one partition): the clock must hold, then backfill, and nothing may be marked missed by mistake.

Prints check-in-to-monitor p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and a line per
tick with its lag: how long after the minute boundary the tick fired.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (CHECKINS, DEFAULT_URL, PARTITIONS, STREAM, VERDICTS, Stalls, Window, ensure_topics,
                    every_minutes, load, log_seconds, msg)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), CHECKINS, VERDICTS)
    out = await (await own_client()).producer(STREAM, VERDICTS, topic_partitions_count=PARTITIONS[VERDICTS])

    M = a.minute_s
    P = PARTITIONS[CHECKINS]
    mons = {}                                   # slug -> [every k minutes, margin, max runtime, next due minute]
    due_at = defaultdict(set)                   # minute when the next run is overdue (due + margin) -> slugs
    open_runs = {}                              # check_in_id -> (slug, minute it times out)
    timeout_at = defaultdict(set)               # minute -> check_in_ids that time out then
    part_clock = {}                             # partition -> minute of the newest message read from it
    last_tick = [None]
    draining = {}                               # partition -> True while its backlog is being read
    outbox = []                                 # verdicts waiting for the sender task
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    caught_up = [False]                         # a new reader first replays what is retained on the lane
    t0 = time.monotonic()
    gc.freeze()                                 # keep start-up objects out of the collector's way

    def verdict(**d):
        outbox.append(SendMessage(msg(**d)))
        stats[d["v"]] += 1

    def expect(slug, nxt):
        m = mons[slug]
        due_at[m[3] + m[1]].discard(slug)
        m[3] = nxt
        due_at[nxt + m[1]].add(slug)

    def run_tick(T, backfill):
        for slug in due_at.pop(T, ()):          # runs overdue at T: the job never checked in
            m = mons[slug]
            verdict(v="missed", slug=slug, due=m[3], tick=T, how="tick")
            m[3] += m[0]
            due_at[m[3] + m[1]].add(slug)
        for cid in timeout_at.pop(T, ()):       # in_progress and nothing since, past max runtime
            slug, _ = open_runs.pop(cid)
            verdict(v="timeout", slug=slug, id=cid, tick=T)
        lag = (time.time() - T * M) * 1000
        stats["ticks"] += 1
        if backfill:
            stats["backfilled"] += 1
        else:
            stats["lag"] = lag
        print(f"[clock] tick minute {T}  lag {lag:,.0f}ms{'  (backfill)' if backfill else ''}  "
              f"missed so far {int(stats['missed_total'] + stats['missed']):,}  "
              f"timed out so far {int(stats['timeout_total'] + stats['timeout']):,}", flush=True)

    def try_tick():
        if len(part_clock) < P:
            return                              # hold until every partition has reported
        g = min(part_clock.values())            # the slowest partition sets the time
        if last_tick[0] is None:
            last_tick[0] = g
            return
        while last_tick[0] < g:
            last_tick[0] += 1
            run_tick(last_tick[0], last_tick[0] < g)

    def on_check_in(d, minute):
        slug, cfg = d["monitor_slug"], d.get("monitor_config") or {}
        if d["status"] == "in_progress":
            if slug not in mons:                # first check-in: the monitor starts here
                mons[slug] = [every_minutes(cfg.get("schedule", "* * * * *")), cfg.get("checkin_margin", 1),
                              cfg.get("max_runtime", 30), None]
            m = mons[slug]
            due = (minute // m[0]) * m[0]       # the run this check-in belongs to
            if m[3] is not None:
                for skipped in range(m[3], due, m[0]):
                    verdict(v="missed", slug=slug, due=skipped, tick=minute, how="next check-in")
            if m[3] is None or due + m[0] > m[3]:
                if m[3] is None:
                    m[3] = due
                    due_at[due + m[1]].add(slug)
                expect(slug, due + m[0])
            t = minute + m[2]
            open_runs[d["check_in_id"]] = (slug, t)
            timeout_at[t].add(d["check_in_id"])
        else:                                   # ok or error: the job finished
            run = open_runs.pop(d["check_in_id"], None)
            if run:
                timeout_at[run[1]].discard(d["check_in_id"])
                stats["finished"] += 1
                stats["failed"] += d["status"] == "error"
            else:
                stats["late_finish"] += 1       # already marked timed out, or never opened

    def on_message(m, p):
        d = load(m.payload())
        minute = int(log_seconds(m.timestamp()) // M)
        if d["type"] == "check_in":
            age = (time.time_ns() - d["ns"]) / 1e6
            if draining.get(p):
                if age < a.budget_ms:
                    draining[p] = False         # this partition's backlog has been read
                else:
                    stats["backlog"] += 1
                    stats["backlog_age"] = max(stats["backlog_age"], age)
            if not draining.get(p):
                if caught_up[0]:
                    lat.add(age)
                else:                           # still replaying: track the oldest message seen this second
                    stats["replayed"] += 1
                    stats["max_age"] = max(stats["max_age"], age)
            on_check_in(d, minute)
        else:
            stats["pulses"] += 1
        if minute > part_clock.get(p, -1):
            part_clock[p] = minute
            try_tick()

    def paused(p):
        el = time.monotonic() - t0
        on = p == a.pause_partition and a.pause_at <= el < a.pause_at + a.pause_for
        if on and not draining.get(p):
            draining[p] = True
            print(f"[monitors] pausing reads of partition {p} for {a.pause_for:g} s (a stuck consumer)", flush=True)
        return on

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                if paused(p):
                    continue
                ms = await c.poll_messages(STREAM, CHECKINS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_message(m, p)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def sender():
        async with out:
            while True:
                await asyncio.sleep(0.01)
                if outbox:
                    batch = outbox[:]
                    outbox.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report_line():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of messages arrived inside the budget; partitions interleave,
                # so one fresh message alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier check-ins this second, "
                      f"monitors known {len(mons):,}, partitions reporting {len(part_clock)}/{P}", flush=True)
                for k in ("replayed", "max_age"):
                    stats[k] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            wall = int(time.time() // M)
            behind = wall - (last_tick[0] if last_tick[0] is not None else wall)
            held = ""
            if behind > 0 and part_clock and time.time() - wall * M > 1.0:
                slow = min(part_clock, key=part_clock.get)
                held = f"  clock held by partition {slow} ({behind} min behind)"
            back = (f"  backlog read {int(stats['backlog']):,} check-ins up to {stats['backlog_age'] / 1000:.1f}s old"
                    if stats["backlog"] else "")
            print(f"[{a.name}] {r['n']:>6,} check-ins/s  check-in-to-monitor p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  monitors {len(mons):,}  jobs running "
                  f"{len(open_runs):,}  clock minute {last_tick[0]}  ticks {int(stats['ticks'])}"
                  f"{held}{back}  missed {int(stats['missed']):,}  timed out {int(stats['timeout']):,}  "
                  f"verdicts sent {int(stats['sent']):,}  {stalls.read()}", flush=True)
            stats["missed_total"] += stats["missed"]
            stats["timeout_total"] += stats["timeout"]
            for k in ("missed", "timeout", "sent", "ticks", "backfilled", "finished", "failed", "late_finish",
                      "pulses", "backlog", "backlog_age"):
                stats[k] = 0

    parts = list(range(P))
    await asyncio.gather(*(read(parts[r::a.readers]) for r in range(a.readers)),
                         sender(), report_line(), stalls.watch())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="monitors")
    p.add_argument("--budget-ms", type=int, default=50, help="check-in-to-monitor p99 budget for the lane")
    p.add_argument("--minute-s", type=float, default=60, help="seconds in a clock minute (CI compresses it)")
    p.add_argument("--readers", type=int, default=2, help="connections reading the lane's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--pause-partition", type=int, default=-1, help="stop reading this partition for a while")
    p.add_argument("--pause-at", type=float, default=90, help="seconds after start the pause begins")
    p.add_argument("--pause-for", type=float, default=0, help="seconds the partition is not read")
    asyncio.run(main(p.parse_args()))
