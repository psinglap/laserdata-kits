"""The pacing controllers, fed by the spend lane.

Reads the spend topic on its own connection and consumer group. Each record is one bid server's running
spend for one ad group in the current flight (the resulting total, with a sequence), so the ad group's
spend is the sum of the latest totals from every bid server, and replays or repeats change nothing.

For every ad group it decides, as spend arrives:
  * PAUSE the moment spend reaches the flight budget;
  * THROTTLE (bid on about half the opportunities) while spend runs more than --ahead ahead of an even
    pace through the flight, and OPEN again once it is back on pace;
and publishes each change on pacing-decisions, which every bid server reads (bidders.py).

Prints spend-to-pacing p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business
number: overspend, the USD won on ad groups after they had already reached their budget, because the
spend or the pause was still in flight.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy, SendMessage

from common import (DEFAULT_URL, PACING, PARTITIONS, SPEND, STREAM, Window, ad_group, budget, ensure_topics,
                    flight, load, msg)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SPEND, PACING)
    spend = await (await own_client()).consumer_group(
        f"{a.name}-{SPEND}-{os.getpid()}", STREAM, SPEND, polling_strategy=PollingStrategy.First(),
        batch_length=5000, auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))
    out = await (await own_client()).producer(STREAM, PACING, topic_partitions_count=PARTITIONS[PACING])
    groups = [ad_group(i) for i in range(a.ad_groups)]
    budgets = {g: budget(g, a.wins_per_s, a.ad_groups, a.flight_s) for g in groups}
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (bid server, ad group) -> last sequence applied
    total = defaultdict(float)              # (flight, ad group) -> spend: sum of every bid server's latest total
    last = {}                               # (flight, bid server, ad group) -> that server's latest total
    state = {}                              # (flight, ad group) -> "open" | "throttle" | "pause"
    dseq = defaultdict(int)                 # ad group -> decision sequence
    pending = []                            # decisions waiting for the next send (one batch per tick)
    over = defaultdict(float)               # flight -> USD won after the ad group reached its budget
    paused_n = defaultdict(int)             # flight -> ad groups paused
    caught_up = [False]                     # a new reader first replays spend from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def decide(f, g, new):
        if state.get((f, g), "open") == new:
            return
        state[(f, g)] = new
        dseq[g] += 1
        pending.append(SendMessage(msg(g=g, flight=f, seq=dseq[g], action=new)))
        if new == "pause":
            paused_n[f] += 1

    async def on_spend(m):
        s = load(m.payload())
        now = time.time_ns()
        age = (now - s["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = (s["b"], s["g"])
        if s["seq"] <= seq.get(k, 0):
            stats["dropped"] += 1           # older or repeated total: the newest wins
            return
        seq[k] = s["seq"]
        f, g = s["flight"], s["g"]
        before = total[(f, g)]
        total[(f, g)] += s["spend"] - last.get((f, s["b"], g), 0.0)
        last[(f, s["b"], g)] = s["spend"]
        stats["events"] += 1
        b = budgets[g]
        if before >= b:
            over[f] += total[(f, g)] - before   # won after the ad group had already reached its budget
        if f != flight(a.flight_s, now / 1e9):
            return                          # an earlier flight: count it, but decide nothing
        if total[(f, g)] >= b:
            decide(f, g, "pause")
        elif state.get((f, g)) != "pause" and (elapsed := (now / 1e9 % a.flight_s) / a.flight_s) >= 0.2:
            ahead = total[(f, g)] / (b * elapsed)
            if ahead > 1 + a.ahead:
                decide(f, g, "throttle")
            elif ahead < 1 and state.get((f, g)) == "throttle":
                decide(f, g, "open")

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

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of spend arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier spend records this second",
                      flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            f = flight(a.flight_s)
            spent = sum(v for (ff, _), v in total.items() if ff == f)
            print(f"[{a.name}] {r['n']:>7,} spend events/s  spend-to-pacing p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  decisions sent {int(stats['decisions']):,}", flush=True)
            print(f"[budget] flight {f % 10000}: spent ${spent:,.2f}  ad groups paused {paused_n[f]:,}/{a.ad_groups:,}  "
                  f"throttled {sum(1 for (ff, _), v in state.items() if ff == f and v == 'throttle'):,}  "
                  f"overspend ${over[f]:,.4f} ({100 * over[f] / spent if spent else 0:.3f}% of spend)", flush=True)
            stats.clear()
            old = f - 2                     # forget flights that ended more than one flight ago
            for d in (total, state):
                for k in [k for k in d if k[0] <= old]:
                    del d[k]
            for k in [k for k in last if k[0] <= old]:
                del last[k]

    await asyncio.gather(spend.consume_messages(on_spend, asyncio.Event()), send_decisions(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="pacing")
    p.add_argument("--budget-ms", type=int, default=50, help="spend-to-pacing p99 budget for the lane")
    p.add_argument("--ad-groups", type=int, default=5_000, help="must match bidders.py")
    p.add_argument("--wins-per-s", type=float, default=10_000, help="must match bidders.py (sets the budgets)")
    p.add_argument("--flight-s", type=float, default=60, help="seconds per compressed flight; must match bidders.py")
    p.add_argument("--ahead", type=float, default=0.25, help="throttle when spend runs this far ahead of an even pace")
    asyncio.run(main(p.parse_args()))
