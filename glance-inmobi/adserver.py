"""A Glance ad server that fills phones' prefetch refills under each user's frequency caps and dismissals,
from a view kept current by the lane.

Each process is one ad server; run several (ci.sh runs three). It reads every partition of user-ad-signals
directly (its own offsets, no consumer group) on its own connection and applies every beacon: a user's count
for a key is the sum of the latest totals from every collector. On a second connection it reads its own
partition of prefetch-refills. For each refill it places --slots ads: the first campaigns the user has seen
fewer than --cap times in this cap window, from advertisers the user has not dismissed in it.

On start it rebuilds its view from the log, from the start of the previous cap window, before it takes any
refill. Prints a line a second:
  * beacon-to-ad-server latency (beacon received by the collector -> applied here), PASS or FAIL against
    --budget-ms;
  * refills, ads placed, campaigns skipped at their cap or for a dismissed advertiser, sequence gaps;
  * the business number: stale placements, ads placed for a user after a beacon that rules them out had
    already reached the collector, because it was still on its way here. Each one is an impression past
    the cap or an ad the user just swiped away, back on the lock screen.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, PollingStrategy

from common import (DEFAULT_URL, PARTITIONS, REFILLS, SIGNALS, STREAM, Window, advertiser_of, campaign, cap_window,
                    connect, ensure_topics, load)


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), SIGNALS, REFILLS)
    reader, requests = await connect(a.url), await connect(a.url)     # the lane and the refills, one connection each
    P = PARTITIONS[SIGNALS]
    camps = [campaign(i) for i in range(a.campaigns)]
    advs = [advertiser_of(c) for c in camps]
    rng = random.Random(me)
    start_ns = time.time_ns()

    # Per cap window, so a closed window is forgotten in one step, not by scanning every key.
    total = defaultdict(dict)               # window -> {(user, key): total over every collector}
    last_n = defaultdict(dict)              # window -> {(collector, user, key): that collector's latest total}
    last_seq = {}                           # (collector, user) -> last sequence applied
    recent = defaultdict(list)              # user -> [[decided ns, window, campaign, advertiser, stale], ...]
    lat, st, run = Window(), defaultdict(int), defaultdict(int)   # per-second / whole-run counters
    live, caught, held = set(), [None], deque()
    gc.freeze()                             # keep start-up tables out of the collector's way

    def apply(e):
        k = (e["s"], e["u"])
        prev = last_seq.get(k)
        if prev is not None and e["seq"] <= prev:
            run["repeats"] += 1             # a repeat (replay or at-least-once redelivery): count it once
            return
        if prev is not None and e["seq"] > prev + 1:
            run["gaps"] += 1                # a beacon from this collector never arrived
        last_seq[k] = e["seq"]
        w, u, key = e["w"], e["u"], e["k"]
        ln, tw = last_n[w], total[w]
        nk, tk = (e["s"], u, key), (u, key)
        d = e["n"] - ln.get(nk, 0)
        if d <= 0:
            return
        ln[nk] = e["n"]
        before = tw.get(tk, 0)
        tw[tk] = before + d
        limit = a.cap if e["t"] == "imp" else 1          # a cap for a campaign, one dismiss for an advertiser
        if before < limit <= before + d:
            for pl in recent.get(u, ()):    # placed after this beacon reached the collector: it was stale
                if not pl[4] and pl[0] > e["ns"] and pl[1] == w and key in (pl[2], pl[3]):
                    pl[4] = True
                    st["stale"] += 1
                    run["stale"] += 1

    def release(now):
        # --hold-ms: apply beacons only once they are this old (a slower pipeline)
        while held and now - held[0]["ns"] >= a.hold_ms * 1_000_000:
            e = held.popleft()
            lat.add((now - e["ns"]) / 1e6)
            apply(e)

    async def read():
        who = {p: Consumer.Single(f"{me}-{p}") for p in range(P)}
        since = PollingStrategy.Timestamp(int((cap_window(a.window_s) - 1) * a.window_s * 1e6))
        nxt = {}
        while True:
            more = False
            for p in range(P):
                ms = await reader.poll_messages(STREAM, SIGNALS, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        e = load(m.payload())
                        if caught[0] is None or e["ns"] < start_ns:
                            run["replayed"] += 1    # rebuilding: not live latency
                            apply(e)
                        elif a.hold_ms:
                            held.append(e)
                        else:
                            lat.add((now - e["ns"]) / 1e6)
                            apply(e)
                if len(ms) == a.batch:
                    more = True             # a replay or a burst: read on at once
                elif p not in live:
                    live.add(p)             # this partition has been read to the end of the log once
                    if len(live) == P:
                        caught[0] = time.time_ns()
                        users_seen = len({u for tw in total.values() for (u, _) in tw})
                        print(f"[{a.name}] rebuilt the ad-state view from the log in {(caught[0] - start_ns) / 1e9:.2f}s: "
                              f"{users_seen:,} users, {run['replayed']:,} beacons since the previous window began",
                              flush=True)
            if held:
                release(time.time_ns())
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def fill(u):
        # The ad decision: the first --slots campaigns under their cap, from advertisers the user has not dismissed.
        w = cap_window(a.window_s)
        tw, placed, seen = total[w], 0, set()
        for _ in range(a.tries):
            j = int(a.campaigns * rng.random() ** 3)
            c, adv = camps[j], advs[j]
            if c in seen:
                continue
            seen.add(c)
            if tw.get((u, adv), 0) >= 1:
                st["dismissed"] += 1
                continue
            if tw.get((u, c), 0) >= a.cap:
                st["capped"] += 1
                continue
            recent[u].append([time.time_ns(), w, c, adv, False])
            st["placed"] += 1
            run["placed"] += 1
            placed += 1
            if placed == a.slots:
                return
        st["unfilled"] += a.slots - placed  # those slots go to house content instead

    async def serve():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never take a refill before the view is rebuilt
        who = Consumer.Single(f"{me}-refills")
        strategy, pid = PollingStrategy.Timestamp(caught[0] // 1000), a.pod
        while True:
            ms = await requests.poll_messages(STREAM, REFILLS, consumer=who, partition_id=pid, count=a.batch,
                                              auto_commit=False, polling_strategy=strategy)
            if ms:
                strategy = PollingStrategy.Offset(ms[-1].offset() + 1)
                for m in ms:
                    fill(load(m.payload())["u"])
                    st["refills"] += 1
            else:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        keep_ns = int((a.hold_ms + 5000) * 1e6)
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the ad-state view from the log: {run['replayed']:,} beacons so far",
                      flush=True)
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            pct = 100 * run["stale"] / run["placed"] if run["placed"] else 0.0
            print(f"[{a.name}] beacons {r['n']:>6,}/s  beacon-to-ad-server p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  "
                  f"p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  refills {st['refills']:,}  ads placed "
                  f"{st['placed']:,}  skipped at cap {st['capped']:,}  dismissed {st['dismissed']:,}  stale placements "
                  f"{st['stale']}  this run {run['stale']:,} of {run['placed']:,} ({pct:.3f}%)  gaps {run['gaps']}",
                  flush=True)
            st.clear()
            cut = time.time_ns() - keep_ns  # a beacon held past this would not be checked; none is
            for u in list(recent):
                kept = [pl for pl in recent[u] if pl[0] >= cut]
                if kept:
                    recent[u] = kept
                else:
                    del recent[u]
            w = cap_window(a.window_s)
            for d in (total, last_n):       # forget windows that ended more than one window ago
                for k in [k for k in d if k < w - 1]:
                    del d[k]

    await asyncio.gather(read(), serve(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="ad-1")
    p.add_argument("--pod", type=int, default=0, help="which refill partition this ad server takes (0-7)")
    p.add_argument("--campaigns", type=int, default=40, help="campaigns running; same as the collector")
    p.add_argument("--cap", type=int, default=3, help="impressions per user per campaign per cap window")
    p.add_argument("--window-s", type=float, default=60, help="cap window in seconds (a compressed day); same everywhere")
    p.add_argument("--slots", type=int, default=3, help="ads placed in each prefetch refill")
    p.add_argument("--tries", type=int, default=12, help="campaigns tried before the rest of a refill goes unfilled")
    p.add_argument("--budget-ms", type=float, default=50, help="beacon-to-ad-server p99 budget")
    p.add_argument("--hold-ms", type=float, default=0,
                   help="what-if: apply beacons only after this delay, like a micro-batch pipeline")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
