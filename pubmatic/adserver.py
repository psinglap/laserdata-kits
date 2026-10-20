"""A CTV ad server that fills ad slots under each household's frequency cap and shares every impression
with every other ad server through the lane.

Each process is one ad server; run several (ci.sh runs three). It fills --rate ad slots a second. For each
slot it picks a household (viewing is skewed: a small share of heavy-viewing, multi-screen households makes
a large share of the requests) and the first campaign that household has not yet seen --cap times in this
cap window, by this server's view. Every impression goes on household-exposures, keyed by household: the
server's own running count of that campaign for that household in the window (the resulting total, never
+1), with a per-(server, household) sequence.

The server reads every partition of the lane directly (its own offsets, no consumer group) on its own
connection, so it sees every other server's impressions and applies them to its view: a household's count
for a campaign is the sum of the latest totals from every ad server. Its own impressions count at once.

On start it rebuilds its view from the log, from the start of the previous cap window, before it fills any
slot. Prints a line a second:
  * exposure-to-ad-server latency (impression served on another server -> applied here), PASS or FAIL
    against --budget-ms;
  * campaigns skipped because the household was at its cap (the cap at work), unfilled slots, sequence gaps;
and, when a cap window closes, the business number: over-cap impressions, served to a household that had
already seen the campaign --cap times, because an earlier impression was still on its way.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, EXPOSURES, PARTITIONS, STREAM, Window, campaign, cap_window, connect,
                    ensure_topics, household, load, msg, part)


async def main(a):
    me = f"{a.name}.{os.getpid()}"          # a restarted server is a new writer, so its counts start again
    await ensure_topics(await connect(a.url), EXPOSURES)
    writer, reader = await connect(a.url), await connect(a.url)     # one connection each way
    P = PARTITIONS[EXPOSURES]
    homes = [household(i) for i in range(a.households)]
    hpart = [part(h, P) for h in homes]
    camps = [campaign(i) for i in range(a.campaigns)]
    heavy_n = max(1, int(a.households * a.heavy_share))
    rng = random.Random(me)
    start_ns = time.time_ns()

    total = {}                              # (window, household, campaign) -> impressions, all ad servers
    last_n = {}                             # (writer, household, campaign, window) -> that writer's latest total
    last_seq = {}                           # (writer, household) -> last sequence applied
    my_seq = defaultdict(int)               # household -> this server's sequence
    my_n = defaultdict(int)                 # (household, campaign, window) -> this server's count
    served, over = defaultdict(int), defaultdict(int)   # window -> impressions; over-cap impressions
    lat, st, run = Window(), defaultdict(int), defaultdict(int)  # per-second / whole-run counters
    live, caught, held = set(), [None], deque()
    gc.freeze()                             # keep start-up tables out of the collector's way

    def apply(e):
        k = (e["s"], e["h"])
        prev = last_seq.get(k)
        if prev is not None and e["seq"] <= prev:
            run["repeats"] += 1             # a repeat (replay or at-least-once redelivery): count it once
            return
        if prev is not None and e["seq"] > prev + 1:
            run["gaps"] += 1                # an impression from this server never arrived
        last_seq[k] = e["seq"]
        nk = (e["s"], e["h"], e["c"], e["w"])
        d = e["n"] - last_n.get(nk, 0)
        if d <= 0:
            return
        last_n[nk] = e["n"]
        tk = (e["w"], e["h"], e["c"])
        before = total.get(tk, 0)
        total[tk] = before + d
        served[e["w"]] += d
        if before + d > a.cap:
            over[e["w"]] += before + d - max(before, a.cap)     # only impressions past the cap count

    def release(now):
        # --hold-ms: apply other servers' impressions only once they are this old (a slower pipeline)
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
                ms = await reader.poll_messages(STREAM, EXPOSURES, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        e = load(m.payload())
                        if e["s"] == me:
                            continue        # this server's own impressions were applied when served
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
                        caught[0] = time.monotonic()
                        homes_seen = len({h for (_, h, _) in total})
                        print(f"[{a.name}] rebuilt the cap view from the log in {(time.time_ns() - start_ns) / 1e9:.2f}s: "
                              f"{homes_seen:,} households, {run['replayed']:,} impressions since the previous window began",
                              flush=True)
            if held:
                release(time.time_ns())
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def serve():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never fill a slot before the view is rebuilt
        t0 = due = time.monotonic()
        owed = 0.0
        while True:
            t = time.monotonic() - t0
            x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
            owed += a.rate * x * a.tick_ms / 1000
            w = cap_window(a.window_s)
            batches = defaultdict(list)
            for _ in range(int(owed)):
                i = rng.randrange(heavy_n) if rng.random() < a.heavy_traffic else rng.randrange(a.households)
                h = homes[i]
                for _ in range(a.tries):    # the ad decision: the first eligible campaign under its cap
                    c = camps[int(a.campaigns * rng.random() ** 2)]
                    if total.get((w, h, c), 0) < a.cap:
                        break
                    st["capped"] += 1
                else:
                    st["unfilled"] += 1     # every campaign tried was at its cap: the slot goes elsewhere
                    continue
                my_seq[h] += 1
                my_n[(h, c, w)] += 1
                e = {"s": me, "h": h, "c": c, "w": w, "seq": my_seq[h], "n": my_n[(h, c, w)], "ns": time.time_ns()}
                apply(e)                    # this server knows its own impression at once
                batches[hpart[i]].append(SendMessage(msg(**e)))
                st["slots"] += 1
            owed -= int(owed)
            # One batch per partition, all sent at once: a household's impressions stay in order, one round trip a tick.
            if batches:
                await asyncio.gather(*(writer.send_messages(STREAM, EXPOSURES, Partitioning.partition_id(p), ms)
                                       for p, ms in batches.items()))
            st["peak"] = int(x > 1)
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        closed = cap_window(a.window_s) - 1     # windows summarised so far (none before start)
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the cap view from the log: {run['replayed']:,} impressions so far", flush=True)
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            phase = "peak" if st["peak"] else "steady"
            print(f"[{a.name}] {st['slots']:>6,} slots filled/s ({phase})  from other servers {r['n']:>6,}/s  "
                  f"exposure-to-ad-server p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  "
                  f"{ok} (<{a.budget_ms:g}ms)  capped {st['capped']:,}  unfilled {st['unfilled']:,}  gaps {run['gaps']}",
                  flush=True)
            st.clear()
            w = cap_window(a.window_s)
            if w - 1 > closed and time.time() - w * a.window_s >= 1.0 + a.hold_ms / 1000:   # its tally is in by now
                c = closed = w - 1
                n, o = served[c], over[c]
                at_cap = sum(1 for (ww, _, _), v in total.items() if ww == c and v >= a.cap)
                print(f"[cap] window {c % 10000} closed: impressions {n:,}  household-campaign pairs at cap {at_cap:,}  "
                      f"over-cap impressions {o:,} ({100 * o / n if n else 0:.3f}%)", flush=True)
                for d in (total, last_n, my_n):     # forget windows that ended more than one window ago
                    for k in [k for k in d if (k[0] if d is total else k[-1]) < c]:
                        del d[k]

    await asyncio.gather(read(), serve(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="ad-1")
    p.add_argument("--rate", type=float, default=5_000, help="ad slots this server fills a second")
    p.add_argument("--households", type=int, default=200_000, help="households watching; same on every server")
    p.add_argument("--heavy-share", type=float, default=0.01, help="share of households that are heavy, multi-screen viewers")
    p.add_argument("--heavy-traffic", type=float, default=0.3, help="share of ad requests that come from them")
    p.add_argument("--campaigns", type=int, default=40, help="campaigns eligible for these slots")
    p.add_argument("--cap", type=int, default=3, help="impressions per household per campaign per cap window")
    p.add_argument("--window-s", type=float, default=60, help="cap window in seconds (a compressed day); same everywhere")
    p.add_argument("--tries", type=int, default=4, help="campaigns tried before a slot is left unfilled")
    p.add_argument("--budget-ms", type=float, default=50, help="exposure-to-ad-server p99 budget")
    p.add_argument("--hold-ms", type=float, default=0,
                   help="what-if: apply other servers' impressions only after this delay, like a micro-batch pipeline")
    p.add_argument("--peak", type=float, default=1, help="slot-rate multiplier during the peak (a live-sports break)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after serving starts")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    p.add_argument("--tick-ms", type=float, default=10, help="how often the server sends its impressions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
