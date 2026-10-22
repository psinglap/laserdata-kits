"""BCAP, the budget controller, fed by the billable-events lane.

Reads every partition of billable-events directly, on --readers connections of its own, from the start of the
log, tracking its own position (no consumer group: a group reader visits one partition per poll, so with 16
partitions an event waits for its partition's turn). Campaigns are keyed to partitions, so every event of a
campaign, and every repeat of it, reaches the same reader in order.

For every event it applies your rules:
  * dedupe on the canonical impression ID plus its 1-minute event-time bucket, so a view or click sent twice
    (network retry, scroll jitter, pre-fetch) is charged once;
  * drop events that arrive more than 60 minutes after their event time (an app kept them in the background);
  * add the cost to the campaign's spend for the budget day, and STOP the campaign the moment spend reaches its
    daily budget, on budget-stops, which every ad server reads (ad_servers.py).
Every partition also carries watermark marks from the collectors. BCAP tracks how far behind its slowest
partition is (the lane's lag); past --max-lag-ms it goes fail-safe and stops campaigns already at
--failsafe-at of their budget, as your post describes BCAP doing from watermark progress.

Prints billable-to-BCAP p50/p99/p99.9 every second with PASS/FAIL against --budget-ms, and the business numbers:
over-burn (₹ charged on campaigns after they had already reached their budget, because the event or the stop
was still in flight), repeats and stale events dropped, and the watermark lag.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (BILLABLE, BUCKET_S, DEFAULT_URL, MAX_LATE_S, PARTITIONS, STOPS, STREAM, Window, budget,
                    campaign, day, ensure_topics, load, msg)


def canonical(raw: str) -> str:
    # Ad sources write the same impression ID in different shapes ("IMP_00042", "imp-00042"); one canonical form.
    return raw.strip().lower().replace("_", "-")


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await own_client()
    await ensure_topics(admin, BILLABLE, STOPS)
    parts = list(range((await admin.get_topic(STREAM, BILLABLE)).partitions_count))
    out = await (await own_client()).producer(STREAM, STOPS, topic_partitions_count=PARTITIONS[STOPS])
    if a.budgets_csv:                       # a shadow run: your campaigns' daily budgets, "campaign,budget_inr"
        budgets = defaultdict(lambda: float("inf"))
        for line in open(a.budgets_csv):
            c, _, b = line.strip().partition(",")
            try:
                budgets[c] = float(b)
            except ValueError:
                continue                    # a header or a blank line
        camps = list(budgets)
    else:
        camps = [campaign(i) for i in range(a.campaigns)]
        budgets = {c: budget(c, a.events_per_s, a.campaigns, a.day_s) for c in camps}
    lat, stats = Window(), defaultdict(float)
    spend = defaultdict(float)              # (day, campaign) -> ₹ charged
    state = {}                              # (day, campaign) -> "stop" (absent: serving)
    dseq = defaultdict(int)                 # campaign -> decision sequence
    pending = []                            # decisions waiting for the next send (one batch per tick)
    over = defaultdict(float)               # day -> ₹ charged after the campaign had reached its budget
    over_n = defaultdict(int)
    charged = defaultdict(float)            # day -> ₹ charged
    stopped = defaultdict(int)              # day -> campaigns stopped
    seen = [set() for _ in range(a.dedupe_keep)]   # dedupe keys, one set per minute of arrival, newest first
    rotate_at = [time.monotonic() + BUCKET_S]
    progress = {p: 0 for p in parts}        # partition -> newest send time applied (events and watermark marks)
    failsafe = [False]
    caught_up = [False]                     # a new reader first replays events from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def decide(d, c, why):
        if state.get((d, c)) == "stop":
            return
        state[(d, c)] = "stop"
        dseq[c] += 1
        pending.append(SendMessage(msg(c=c, day=d, seq=dseq[c], action="stop", why=why)))
        stopped[d] += 1

    def on_event(m, p, now):
        e = load(m.payload())
        progress[p] = max(progress[p], e["ns"])
        if "wm" in e:
            return                          # a watermark mark: progress only
        age = (now - e["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest event seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        if now // 1_000_000 - e["et"] > MAX_LATE_S * 1000:
            stats["stale"] += 1             # older than an hour: dropped, your rule
            stats["stale_inr"] += e["cost"]
            return
        key = (canonical(e["id"]), e["et"] // (BUCKET_S * 1000))
        if any(key in s for s in seen):
            stats["repeats"] += 1           # the same view or click again: charged once
            stats["repeats_inr"] += e["cost"]
            return
        seen[0].add(key)
        d, c = e["day"], e["c"]
        before = spend[(d, c)]
        spend[(d, c)] += e["cost"]
        charged[d] += e["cost"]
        stats["events"] += 1
        if e["kind"] == "click":
            stats["clicks"] += 1
        b = budgets[c]
        if before >= b:
            over[d] += e["cost"]           # charged after the campaign had already reached its budget
            over_n[d] += 1
        if d != day(a.day_s, now / 1e9):
            return                          # an earlier day: count it, but decide nothing
        if spend[(d, c)] >= b:
            decide(d, c, "budget")
        elif failsafe[0] and spend[(d, c)] >= a.failsafe_at * b:
            decide(d, c, "failsafe")

    async def read(pids):
        # Each partition in turn, from the start of the log (spend is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, BILLABLE, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_event(m, p, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_decisions():
        # Stops are small and come in bursts; one batch every 2 ms keeps them off a send-per-decision loop.
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
            if time.monotonic() >= rotate_at[0]:   # keep --dedupe-keep minutes of dedupe keys
                seen.insert(0, set())
                seen.pop()
                rotate_at[0] += BUCKET_S
            now = time.time_ns()
            lag = (now - min(progress.values())) / 1e6 if all(progress.values()) else 0.0
            if not caught_up[0]:
                # caught up once a whole second of events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier events this second",
                      flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            was = failsafe[0]
            failsafe[0] = lag > a.max_lag_ms
            if failsafe[0] and not was:
                print(f"[{a.name}] FAIL-SAFE: slowest partition {lag:,.0f} ms behind; stopping campaigns at "
                      f"{a.failsafe_at:.0%} of budget", flush=True)
                d = day(a.day_s)
                for c in camps:
                    if spend[(d, c)] >= a.failsafe_at * budgets[c]:
                        decide(d, c, "failsafe")
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            d = day(a.day_s)
            print(f"[{a.name}] {r['n']:>7,} billable events/s  billable-to-BCAP p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"watermark lag {lag:,.0f}ms{'  FAIL-SAFE' if failsafe[0] else ''}  stops sent {int(stats['decisions']):,}",
                  flush=True)
            print(f"[budget] day {d % 10000}: charged ₹{charged[d]:,.2f}  campaigns stopped {stopped[d]:,}/{len(camps):,}  "
                  f"over-burn ₹{over[d]:,.2f} ({100 * over[d] / charged[d] if charged[d] else 0:.3f}% of spend, "
                  f"{over_n[d]:,} events)  repeats dropped {int(stats['repeats']):,} (₹{stats['repeats_inr']:,.2f})  "
                  f"stale dropped {int(stats['stale']):,}", flush=True)
            stats.clear()
            old = d - 2                     # forget days that ended more than one day ago
            for m in (spend, state):
                for k in [k for k in m if k[0] <= old]:
                    del m[k]

    await asyncio.gather(send_decisions(), report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="bcap")
    p.add_argument("--budget-ms", type=int, default=50, help="billable-to-BCAP p99 budget for the lane")
    p.add_argument("--campaigns", type=int, default=10_000, help="must match ad_servers.py")
    p.add_argument("--events-per-s", type=float, default=10_000, help="must match ad_servers.py (sets the budgets)")
    p.add_argument("--day-s", type=float, default=60, help="seconds per compressed budget day; must match ad_servers.py")
    p.add_argument("--budgets-csv", default="", help="shadow run: daily budgets per campaign, lines of campaign,budget_inr")
    p.add_argument("--readers", type=int, default=2, help="connections reading the billable partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--dedupe-keep", type=int, default=3,
                   help="minutes of dedupe keys kept in memory (your Flink state keeps the full hour)")
    p.add_argument("--max-lag-ms", type=float, default=2000, help="fail-safe when the slowest partition is this far behind")
    p.add_argument("--failsafe-at", type=float, default=0.9, help="in fail-safe, stop campaigns at this share of budget")
    asyncio.run(main(p.parse_args()))
