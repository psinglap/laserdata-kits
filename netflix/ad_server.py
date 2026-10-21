"""An ad server with its frequency-cap view: it decides ads under each profile's frequency caps and shares
every impression with every other ad server through the frequency-cap lane.

Each process is one ad server; run several (ci.sh runs three) with --index 0, 1, 2 and the same --servers.
Two kinds of ad decision:
  * on demand: --rate ad slots a second for profiles watching series and films. Viewing is skewed: a small
    share of binge-watching profiles makes a large share of the requests. Each slot gets the first campaign
    the profile has not yet seen --cap times in this cap window, by this server's view.
  * a live ad break: every --live-every-s seconds (on the wall clock, so every server agrees), each of the
    --live-profiles profiles watching the live event reaches the break within --ramp-ms, and its pod of
    --pod slots is decided one slot every --slot-gap-ms, each slot by a different ad server (the next
    request lands wherever the load balancer sends it). Each slot gets the first sponsor the profile has not
    yet seen --live-cap times in this window. The cap only holds if the previous slot's impression, decided
    on another server, is already in this server's view.

Every impression goes on fcap-impressions, keyed by profile: the server's own running count of that campaign
for that profile in the window (the resulting total, never +1), with a per-(server, profile) sequence; the
server and sequence are the event ID. With --dup-rate, a share of impressions is delivered twice, as an
at-least-once pipeline does; readers drop the repeat by its event ID, so nothing is counted twice.

The server reads every partition of the lane directly (its own offsets, no consumer group) on its own
connection and applies the other servers' impressions to its view. Its own count at once. On start it
rebuilds its view from the log, from the start of the previous cap window, before it decides any ad.

Prints a line a second: impression-to-cap-view latency (published on another server -> applied here), PASS
or FAIL against --budget-ms, campaigns skipped at their cap, unfilled slots, repeats dropped, sequence gaps.
When a cap window closes, the business numbers: over-cap impressions, and live sponsors shown to a profile
more than --live-cap times, because an earlier impression was still on its way.
"""
import argparse
import asyncio
import gc
import os
import random
import time
import zlib
from collections import defaultdict, deque

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, FCAP, PARTITIONS, STREAM, Window, campaign, cap_window, connect, ensure_topics,
                    load, msg, part, profile, sponsor)


async def main(a):
    assert 0 <= a.index < a.servers, "--index must be in 0..--servers-1"
    assert a.window_s % a.live_every_s == 0, "a live break must not straddle two cap windows"
    assert a.ramp_ms + a.pod * a.slot_gap_ms < a.live_every_s * 1000, "a break's pods must end before the next break"
    me = f"{a.name}.{os.getpid()}"          # a restarted server is a new writer, so its counts start again
    await ensure_topics(await connect(a.url), FCAP)
    writer, reader = await connect(a.url), await connect(a.url)     # one connection each way
    P = PARTITIONS[FCAP]
    pfs = [profile(i) for i in range(a.profiles)]
    ppart = [part(p, P) for p in pfs]
    camps = [campaign(i) for i in range(a.campaigns)]
    spons = [sponsor(i) for i in range(a.sponsors)]
    heavy_n = max(1, int(a.profiles * a.heavy_share))
    rng = random.Random(me)
    start_ns = time.time_ns()

    # Live pods: slot k of live profile i in break b is decided by server (i + k + b) % servers, at the moment
    # the profile reaches the break plus k slot gaps. Precomputed per residue, sorted by time into the break.
    live_tasks = defaultdict(list)
    for i in range(min(a.live_profiles, a.profiles)):
        reach = zlib.crc32(pfs[i].encode()) % max(1, int(a.ramp_ms))
        for k in range(a.pod):
            live_tasks[(i + k) % a.servers].append((reach + k * a.slot_gap_ms, i, k))
    for t in live_tasks.values():
        t.sort()

    # Per cap window, so a closed window is forgotten in one step, not by scanning every key.
    total = defaultdict(dict)               # window -> {(profile, campaign): impressions, all ad servers}
    last_n = defaultdict(dict)              # window -> {(writer, profile, campaign): that writer's latest total}
    last_seq = {}                           # (writer, profile) -> last sequence applied
    my_seq = defaultdict(int)               # profile -> this server's sequence
    my_n = defaultdict(dict)                # window -> {(profile, campaign): this server's count}
    served, live_served, over, live_over, at_cap = (defaultdict(int) for _ in range(5))   # per window
    lat, st, run = Window(), defaultdict(int), defaultdict(int)  # per-second / whole-run counters
    live, caught, held = set(), [None], deque()
    gc.freeze()                             # keep start-up tables out of the collector's way

    def cap_of(e):
        return a.live_cap if e.get("k") == "L" else a.cap         # live sponsors are capped on their own

    def apply(e):
        k = (e["s"], e["p"])
        prev = last_seq.get(k)
        if prev is not None and e["seq"] <= prev:
            run["repeats"] += 1             # same event ID seen before (retry, redelivery, replay): count it once
            return
        if prev is not None and e["seq"] > prev + 1:
            run["gaps"] += 1                # an impression from this server never arrived
        last_seq[k] = e["seq"]
        w, ln, tw = e["w"], last_n[e["w"]], total[e["w"]]
        nk, tk = (e["s"], e["p"], e["c"]), (e["p"], e["c"])
        d = e["n"] - ln.get(nk, 0)
        if d <= 0:
            return
        ln[nk] = e["n"]
        before, cap = tw.get(tk, 0), cap_of(e)
        tw[tk] = before + d
        served[w] += d
        if e.get("k") == "L":
            live_served[w] += d
        if before < cap <= before + d:
            at_cap[w] += 1                  # this profile has now seen the campaign its full cap
        if before + d > cap:
            o = before + d - max(before, cap)           # only impressions past the cap count
            over[w] += o
            if e.get("k") == "L":
                live_over[w] += o

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
                ms = await reader.poll_messages(STREAM, FCAP, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        e = load(m.payload())
                        if e["s"] == me:
                            continue        # this server's own impressions were applied when decided
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
                        seen = len({p for tw in total.values() for (p, _) in tw})
                        print(f"[{a.name}] rebuilt the cap view from the log in {(time.time_ns() - start_ns) / 1e9:.2f}s: "
                              f"{seen:,} profiles, {run['replayed']:,} impressions since the previous window began",
                              flush=True)
            if held:
                release(time.time_ns())
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def decide(i, c, w, mine, kind, batches):
        p = pfs[i]
        my_seq[p] += 1
        n = mine[(p, c)] = mine.get((p, c), 0) + 1
        e = {"s": me, "p": p, "c": c, "w": w, "seq": my_seq[p], "n": n, "k": kind, "ns": time.time_ns()}
        apply(e)                            # this server knows its own impression at once
        m = msg(**e)
        batches[ppart[i]].append(SendMessage(m))
        if a.dup_rate and rng.random() < a.dup_rate:
            batches[ppart[i]].append(SendMessage(m))    # delivered twice, same event ID
            st["dups"] += 1

    async def serve():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never decide an ad before the view is rebuilt
        ready = time.time()
        due = time.monotonic()
        owed, cur_b, tasks, ptr = 0.0, None, [], 0
        while True:
            now = time.time()
            w = cap_window(a.window_s, now)
            tw, mine = total[w], my_n[w]
            batches = defaultdict(list)
            # On demand: --rate slots a second.
            owed += a.rate * a.tick_ms / 1000
            for _ in range(int(owed)):
                i = rng.randrange(heavy_n) if rng.random() < a.heavy_traffic else rng.randrange(a.profiles)
                p = pfs[i]
                for _ in range(a.tries):    # the ad decision: the first eligible campaign under its cap
                    c = camps[int(a.campaigns * rng.random() ** 2)]
                    if tw.get((p, c), 0) < a.cap:
                        break
                    st["capped"] += 1
                else:
                    st["unfilled"] += 1     # every campaign tried was at its cap: the slot goes elsewhere
                    continue
                decide(i, c, w, mine, "V", batches)
                st["slots"] += 1
            owed -= int(owed)
            # Live break: the pod slots that fall due on this server by now.
            b = int(now // a.live_every_s)
            if b != cur_b:
                cur_b, tasks = b, live_tasks[(a.index - b) % a.servers]
                ptr = 0 if b * a.live_every_s >= ready - 0.05 else len(tasks)   # skip a break already under way
            into_ms = (now - b * a.live_every_s) * 1000
            while ptr < len(tasks) and tasks[ptr][0] <= into_ms:
                _, i, k = tasks[ptr]
                ptr += 1
                p = pfs[i]
                first = (i * 7919 + b * 104729) % a.sponsors     # the same preference order on every server
                for j in range(a.sponsors):
                    c = spons[(first + j) % a.sponsors]
                    if tw.get((p, c), 0) < a.live_cap:
                        break
                    st["capped"] += 1
                else:
                    st["unfilled"] += 1
                    continue
                decide(i, c, w, mine, "L", batches)
                st["live"] += 1
            # One batch per partition, all sent at once: a profile's impressions stay in order, one round trip a tick.
            if batches:
                await asyncio.gather(*(writer.send_messages(STREAM, FCAP, Partitioning.partition_id(p), ms)
                                       for p, ms in batches.items()))
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
            phase = "live break" if st["live"] else "on demand"
            print(f"[{a.name}] {st['slots']:>5,} on-demand + {st['live']:>5,} live slots/s ({phase})  "
                  f"from other servers {r['n']:>6,}/s  impression-to-cap-view p50 {r['p50']:5.1f}ms  "
                  f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  capped {st['capped']:,}  "
                  f"unfilled {st['unfilled']:,}  repeats dropped {run['repeats']:,}  gaps {run['gaps']}", flush=True)
            st.clear()
            w = cap_window(a.window_s)
            if w - 1 > closed and time.time() - w * a.window_s >= 1.0 + a.hold_ms / 1000:   # its tally is in by now
                c = closed = w - 1
                n, o, ln, lo = served[c], over[c], live_served[c], live_over[c]
                print(f"[cap] window {c % 10000} closed: impressions {n:,} (live {ln:,})  profile-campaign pairs at cap "
                      f"{at_cap[c]:,}  over-cap impressions {o:,} ({100 * o / n if n else 0:.3f}%)  "
                      f"live sponsor repeats {lo:,} ({100 * lo / ln if ln else 0:.3f}% of live)", flush=True)
                for d in (total, last_n, my_n):     # forget windows that ended more than one window ago
                    for k in [k for k in d if k < c]:
                        del d[k]

    await asyncio.gather(read(), serve(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="ads-1")
    p.add_argument("--index", type=int, default=0, help="this server's index, 0..--servers-1")
    p.add_argument("--servers", type=int, default=3, help="ad servers sharing live pods; same on every server")
    p.add_argument("--rate", type=float, default=1_000, help="on-demand ad slots this server decides a second")
    p.add_argument("--profiles", type=int, default=200_000, help="member profiles watching; same on every server")
    p.add_argument("--heavy-share", type=float, default=0.01, help="share of profiles that are binge-watching")
    p.add_argument("--heavy-traffic", type=float, default=0.3, help="share of on-demand ad requests from them")
    p.add_argument("--campaigns", type=int, default=40, help="on-demand campaigns eligible for these slots")
    p.add_argument("--cap", type=int, default=3, help="impressions per profile per campaign per cap window")
    p.add_argument("--live-profiles", type=int, default=6_000, help="profiles watching the live event")
    p.add_argument("--live-every-s", type=float, default=30, help="seconds between live ad breaks")
    p.add_argument("--ramp-ms", type=float, default=2_000, help="live viewers reach the break within this time")
    p.add_argument("--pod", type=int, default=3, help="ad slots in a live break")
    p.add_argument("--slot-gap-ms", type=float, default=500, help="time between a profile's pod slots (compressed)")
    p.add_argument("--sponsors", type=int, default=12, help="live-event sponsors")
    p.add_argument("--live-cap", type=int, default=1, help="impressions per profile per sponsor per cap window")
    p.add_argument("--window-s", type=float, default=60, help="cap window in seconds (a compressed day); same everywhere")
    p.add_argument("--tries", type=int, default=4, help="on-demand campaigns tried before a slot is left unfilled")
    p.add_argument("--budget-ms", type=float, default=50, help="impression-to-cap-view p99 budget")
    p.add_argument("--dup-rate", type=float, default=0.005, help="share of impressions delivered twice")
    p.add_argument("--hold-ms", type=float, default=0,
                   help="what-if: apply other servers' impressions only after this delay, like a pipeline in retries")
    p.add_argument("--tick-ms", type=float, default=10, help="how often the server sends its impressions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
