"""The allowance tracker, fed by the usage lane: each app's Forge usage this cycle against its allowance.

Reads the usage topic directly, partition by partition, on --readers connections of its own (no consumer
group: a group reader visits one partition per poll, so with 16 partitions a record waits for its
partition's turn). Each record is one runtime host's running usage for one app in the current cycle (the
resulting total in GB-seconds, with a sequence), so the tracker adds only what is new:
  * a record whose sequence it has already applied (a retry, or a replay after a restart) adds nothing;
  * a newer one adds its total minus the last total applied for that host and app, so a record lost in
    between is covered by the next one.
Apache Iggy delivers at least once; counting once comes from this rule, which is why the lane carries totals.

An app's usage is the sum of the latest totals from every host. As it arrives, the tracker checks it against
the app's allowance and publishes each crossing of 50%, 75%, 90% and 100% (and, for apps with a budget,
125%) once per cycle to usage-alerts, which the alert service reads (notifier.py).

Prints usage-to-tracker p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business
numbers: apps past each level this cycle, overage in USD, retries absorbed, and what a counter that adds
every record's own GB-seconds would have over-counted, and how many alerts it would have sent early.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (ALERTS, BUDGET_AT, DEFAULT_URL, LEVELS, MONTHLY_ALLOWANCE_GBS, PARTITIONS, PRICE_PER_GBS,
                    STREAM, USAGE, AppMix, Window, allowance, cycle, cycle_seconds, ensure_topics, has_budget,
                    load, msg)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), USAGE, ALERTS)
    out = await (await own_client()).producer(STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS])
    mix = AppMix(a.apps)
    allow = {}

    def allowance_of(name):
        if name not in allow:               # apps the mix doesn't know (a shadow run) get the real allowance
            allow[name] = (a.allowance_gbs if a.allowance_gbs else
                           allowance(name, mix.share[name], a.invocations_per_s, cycle_seconds(a.cycle_s))
                           if name in mix.share else MONTHLY_ALLOWANCE_GBS)
        return allow[name]

    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (host, app) -> last sequence applied
    last = {}                               # (cycle, host, app) -> that host's latest total
    total = defaultdict(float)              # (cycle, app) -> GB-seconds: sum of every host's latest total
    naive = defaultdict(float)              # (cycle, app) -> what a counter adding every record's d would hold
    level = {}                              # (cycle, app) -> index of the next level to alert
    early = set()                           # (cycle, app, level) a no-dedupe counter would have alerted early
    crossed = defaultdict(int)              # (cycle, level) -> apps past it
    aseq = defaultdict(int)                 # app -> alert sequence
    pending = []                            # alerts waiting for the next send (one batch per tick)
    tot = defaultdict(float)                # since start: absorbed, metered, naive
    caught_up = [False]                     # a new reader first replays usage from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def levels_for(name):
        return LEVELS + ((BUDGET_AT,) if has_budget(name) else ())

    def on_usage(m):
        u = load(m.payload())
        now = time.time_ns()
        age = (now - u["ns"]) / 1e6
        c, name = u["c"], u["a"]
        naive[(c, name)] += u["d"]          # a counter that trusts every record, retries included
        tot["naive"] += u["d"]
        k = (u["h"], name)
        if u["seq"] <= seq.get(k, 0):
            tot["absorbed"] += 1            # a retry or a replay: already counted
            return
        if not caught_up[0]:                # still replaying: track the oldest new record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        seq[k] = u["seq"]
        add = u["gbs"] - last.get((c, u["h"], name), 0.0)
        last[(c, u["h"], name)] = u["gbs"]
        total[(c, name)] += add
        tot["metered"] += add
        stats["applied"] += 1
        if caught_up[0]:
            lat.add(age)                    # live usage, from the host's send to the app's total updated
        if c != cycle(a.cycle_s, now / 1e9):
            return                          # an earlier cycle: count it, but alert nothing
        cap = allowance_of(name)
        lv = levels_for(name)
        i = level.get((c, name), 0)
        while i < len(lv) and total[(c, name)] >= lv[i] / 100 * cap:
            aseq[name] += 1
            pending.append(SendMessage(msg(a=name, c=c, lvl=lv[i], seq=aseq[name], uns=u["ns"],
                                           gbs=round(total[(c, name)], 3), allow=round(cap, 3))))
            crossed[(c, lv[i])] += 1
            i += 1
        level[(c, name)] = i
        for x in lv[i:]:                    # levels the true total hasn't reached but a no-dedupe counter has
            if naive[(c, name)] >= x / 100 * cap:
                early.add((c, name, x))
            else:
                break

    async def send_alerts():
        # Crossings come in bursts; one batch every 2 ms keeps them off a send-per-alert loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["alerts"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, USAGE, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_usage(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of usage arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier usage records this second",
                      flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} usage events/s  usage-to-tracker p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"alerts sent {int(stats['alerts']):,}", flush=True)
            c = cycle(a.cycle_s)
            apps_now = [n for (cc, n) in total if cc == c]
            used = sum(total[(c, n)] for n in apps_now)
            over = sum(max(0.0, total[(c, n)] - allowance_of(n)) for n in apps_now)
            budgeted = sum(1 for n in apps_now if has_budget(n))
            extra = tot["naive"] - tot["metered"]
            print(f"[usage] cycle {c % 10000}: {used:,.0f} GB-s on {len(apps_now):,} apps  "
                  f"past 50% {crossed[(c, 50)]:,}  75% {crossed[(c, 75)]:,}  90% {crossed[(c, 90)]:,}  "
                  f"100% {crossed[(c, 100)]:,}  budget {crossed[(c, BUDGET_AT)]:,}/{budgeted:,}  "
                  f"overage ${over * PRICE_PER_GBS:,.4f}  retries absorbed {int(tot['absorbed']):,}  "
                  f"without dedupe +{extra:,.1f} GB-s ({100 * extra / tot['metered'] if tot['metered'] else 0:.2f}%), "
                  f"early alerts {sum(1 for e in early if e[0] == c):,}", flush=True)
            stats.clear()
            old = c - 2                     # forget cycles that ended more than a cycle ago
            for d in (total, naive, level):
                for k in [k for k in d if k[0] <= old]:
                    del d[k]
            for k in [k for k in last if k[0] <= old]:
                del last[k]
            early.difference_update({e for e in early if e[0] <= old})

    parts = list(range((await (await own_client()).get_topic(STREAM, USAGE)).partitions_count))
    await asyncio.gather(send_alerts(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the usage partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="tracker")
    p.add_argument("--budget-ms", type=int, default=50, help="usage-to-tracker p99 budget for the lane")
    p.add_argument("--apps", type=int, default=5_000, help="must match runtime.py")
    p.add_argument("--invocations-per-s", type=float, default=4_000, help="must match runtime.py (sets allowances)")
    p.add_argument("--cycle-s", type=float, default=60,
                   help="seconds per compressed billing cycle, must match runtime.py; 0 = calendar month")
    p.add_argument("--allowance-gbs", type=float, default=0,
                   help="one allowance for every app, e.g. 200000 in a shadow run; 0 = the kit's scaled allowances")
    asyncio.run(main(p.parse_args()))
