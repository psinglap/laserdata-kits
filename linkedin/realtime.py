"""Real-time processing's side: one instance reading its share of the rt-signals lane, partition by partition.

Instance i of N owns every partition p with p % N == i and reads them directly on one connection (no consumer
group, no rebalance), storing its offset on the broker at most once a second. For each signal it checks the
member's sequence number: a repeat (seq already applied) is dropped, a jump counts the missing signals as lost.
Losing none is the point of the lane.

--restart-at simulates a restart of this instance (a deploy, a crash): it stops reading for --restart-for
seconds and forgets its position, then resumes from the offset it last stored on the broker. Signals sent while
it was away wait in the log; they are counted as held, apart from live latency, and the instance prints
"catching up" until a whole second of signals is inside the budget again. The per-member sequence state is kept
across the simulated restart, as it would be in the processor's own state store.

Prints a line a second: signals applied, signal-to-processing p50/p99/p99.9 with PASS or FAIL against the budget,
and lost, repeats dropped and held in the log.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, PARTITIONS, RT, STREAM, Window, load


async def main(a):
    name = f"realtime-{a.instance}"
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    while not await client.get_topic(STREAM, RT):
        print(f"[{name}] waiting for tracking_api.py to create the topic…", flush=True)
        await asyncio.sleep(1)
    mine = [p for p in range(PARTITIONS[RT]) if p % a.instances == a.instance]
    reader = IggyClient.from_connection_string(a.url)   # its own connection for reads
    await reader.connect()
    start_us = time.time_ns() // 1000 - int(a.lookback_s * 1e6)
    consumers = {p: Consumer.Single(f"{name}-p{p}") for p in mine}
    last = {}                               # member -> last applied seq (in production: the processor's state store)
    run = [None]
    lat, stats, total = Window(), defaultdict(float), defaultdict(int)
    st = {"away": False, "caught_up": False, "stop_ns": 0, "resume_ns": 0, "resumed_at": 0.0}
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_signal(d, now):
        if d["run"] != run[0]:              # a new tracking_api.py run starts its sequences again
            run[0] = d["run"]
            last.clear()
        m, s = d["m"], d["seq"]
        prev = last.get(m)
        if prev is not None:
            if s <= prev:
                stats["repeats"] += 1       # read again after the restart: already applied
                return
            if s > prev + 1:
                stats["lost"] += s - prev - 1
        last[m] = s
        stats["applied"] += 1
        age = (now - d["ns"]) / 1e6
        if st["resume_ns"] and d["ns"] < st["resume_ns"] and not st["caught_up"]:
            stats["held"] += 1              # waited in the log while this instance was away
            stats["held_max_s"] = max(stats["held_max_s"], age / 1000)
        elif st["caught_up"]:
            lat.add(age)
        else:
            stats["replayed"] += 1          # catching up: not live latency
        stats["max_age"] = max(stats["max_age"], age)

    async def read():
        nxt, committed = {}, {}
        while True:
            if st["away"]:
                nxt.clear()                 # a restarted process has lost its in-memory position
                await asyncio.sleep(0.05)
                continue
            more = False
            for p in mine:
                if p in nxt:
                    strat = PollingStrategy.Offset(nxt[p])
                elif st["resume_ns"]:
                    strat = PollingStrategy.Next()          # resume after the offset stored on the broker
                else:
                    strat = PollingStrategy.Timestamp(start_us)
                commit = time.monotonic() - committed.get(p, 0) >= a.commit_s
                ms = await reader.poll_messages(STREAM, RT, consumer=consumers[p], partition_id=p, count=a.batch,
                                                auto_commit=commit, polling_strategy=strat)
                if ms:
                    if commit:
                        committed[p] = time.monotonic()     # stored on the broker at most once a second
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_signal(load(m.payload()), now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def restart():
        if a.restart_at < 0:
            return
        await asyncio.sleep(a.restart_at)
        st["away"], st["stop_ns"], st["caught_up"] = True, time.time_ns(), False
        print(f"[{name}] restarting: partitions {mine} not read for {a.restart_for:g} s", flush=True)
        await asyncio.sleep(a.restart_for)
        st["resume_ns"], st["resumed_at"], st["away"] = time.time_ns(), time.monotonic(), False
        print(f"[{name}] back: resuming each partition after its stored offset", flush=True)

    async def report():
        held_max = 0.0
        while True:
            await asyncio.sleep(1)
            for k in ("applied", "lost", "repeats", "held"):
                total[k] += int(stats[k])
            held_max = max(held_max, stats["held_max_s"])
            if st["away"]:
                print(f"[{name}] away: signals for partitions {mine} wait in the log  "
                      f"totals: lost {total['lost']:,}", flush=True)
            elif not st["caught_up"]:
                # caught up once a whole second of signals arrived inside the budget; partitions interleave,
                # so one fresh signal alone doesn't mean the backlog is done
                if stats["applied"] and stats["max_age"] < a.budget_ms:
                    st["caught_up"] = True
                    if st["resume_ns"]:
                        print(f"[{name}] restart: away {(st['resume_ns'] - st['stop_ns']) / 1e9:.1f} s, "
                              f"held in the log and applied {total['held']:,} signals, oldest {held_max:.1f} s, "
                              f"repeats dropped {total['repeats']:,}, caught up in "
                              f"{time.monotonic() - st['resumed_at']:.1f} s, "
                              f"lost {total['lost']:,}", flush=True)
                print(f"[{name}] catching up: {int(stats['applied']):,} signals applied this second, "
                      f"held {int(stats['held']):,}, replayed {int(stats['replayed']):,}, "
                      f"repeats dropped {int(stats['repeats']):,}", flush=True)
                lat.flush()
            else:
                r = lat.flush()
                ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
                print(f"[{name}] {r['n']:>7,} signals/s  signal-to-processing p50 {r['p50']:5.1f}ms  "
                      f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms}ms)  "
                      f"lost {int(stats['lost']):,}  repeats dropped {int(stats['repeats']):,}", flush=True)
            print(f"[{name}] totals: applied {total['applied']:,}  lost {total['lost']:,}  "
                  f"held in the log {total['held']:,}  repeats dropped {total['repeats']:,}", flush=True)
            stats.clear()

    await asyncio.gather(read(), restart(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--instance", type=int, default=0)
    p.add_argument("--instances", type=int, default=1, help="instances sharing the partitions (p %% N)")
    p.add_argument("--budget-ms", type=int, default=50)
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--poll-ms", type=float, default=0.5, help="wait between rounds when nothing more is waiting")
    p.add_argument("--commit-s", type=float, default=1.0, help="store the offset on the broker this often")
    p.add_argument("--lookback-s", type=float, default=10, help="on start, read signals from this long ago")
    p.add_argument("--restart-at", type=float, default=-1, help="seconds after start (-1: no restart)")
    p.add_argument("--restart-for", type=float, default=12, help="seconds away")
    asyncio.run(main(p.parse_args()))
