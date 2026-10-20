"""A live points node: reads every ball from the lane and keeps every team's fantasy points current.

Every node reads every partition with its own connection and its own offsets (no shared
consumer group), so adding nodes before a big match or at a wicket spike never rebalances or
slows the others: each one gets every ball, in order per match. It holds --teams simulated user
teams (11 of the 22 players; captain 2x, vice-captain 1.5x), spread over the live matches by
popularity, and updates the total of every team a ball touches, as a points service would before
pushing it to the app.

On start it rebuilds every live match from the log (all balls since the line-ups, kept for a
match day), then times live balls. Prints a line a second:
  * ball-to-node latency (ball created at the scoring feed -> every touched team total updated
    here), with PASS or FAIL against --budget-ms;
  * team totals updated: the fan-out, team totals this node would push;
  * pushed after a newer ball existed: points pushed when the feed had already created a newer
    ball for that match (users saw points that were already old);
  * gaps: a missing sequence number for a match (must stay 0);
  * totals off: sampled teams whose running total differs from a full recount (must stay 0).
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, LANE, PARTITIONS, PLAYERS, STREAM, Window, connect, match_ids, popularity, unpack


def build_teams(n_teams: int, matches: list, rng):
    """Teams in half-points: captain x2 -> 4, vice-captain x1.5 -> 3, the rest -> 2."""
    weights = [popularity(i) for i in range(len(matches))]
    team_match, team_picks = [], []
    index = {m: [[] for _ in range(PLAYERS)] for m in matches}      # match -> player -> [(team, multiplier)]
    for t, m in enumerate(rng.choices(matches, weights, k=n_teams)):
        a = rng.sample(range(11), rng.randint(4, 7))                  # 4 to 7 from side A, the rest from side B
        b = rng.sample(range(11, 22), 11 - len(a))
        picks = a + b
        cap, vc = rng.sample(picks, 2)
        mult = [(p, 4 if p == cap else 3 if p == vc else 2) for p in picks]
        team_match.append(m)
        team_picks.append(mult)
        for p, x in mult:
            index[m][p].append((t, x))
    return team_match, team_picks, index


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, LANE):
        print(f"[{a.name}] waiting for scoring.py to create the points lane…", flush=True)
        await asyncio.sleep(1)

    matches = match_ids(a.matches)
    rng = random.Random(a.name)
    team_match, team_picks, index = build_teams(a.teams, matches, rng)
    totals = [0] * a.teams                                  # half-points
    points = {m: [0] * PLAYERS for m in matches}            # latest fantasy points per player, per match
    gc.freeze()                                             # keep startup tables out of the collector's way

    seen, shown = {}, {}                                    # match -> last seq applied; match -> (created ns, shown ns)
    lat, st = Window(), dict(live=0, shown=0, old=0, gaps=0, updates=0, replayed=0, rebuilt=0, max_age=0.0)
    started, head, caught = time.monotonic(), set(), [None, False]   # partitions at the log head; [rebuilt at, timing live]
    budget_ns = a.budget_ms * 1_000_000

    def apply(ms, now):
        newest = {}
        for msg in ms:
            v = unpack(msg.payload())
            m, seq = v["m"], v["s"]
            last = seen.get(m)
            if last is not None:
                if seq <= last:
                    continue                                # a repeat (at-least-once redelivery): skip it
                if seq > last + 1:
                    st["gaps"] += 1                         # a ball for this match never arrived
            seen[m] = seq
            pts, idx = points.get(m), index.get(m)
            if pts is None:
                continue                                    # a match this node holds no teams for
            touched = 0
            for p, total in v["p"]:
                d = total - pts[p]
                if d:
                    pts[p] = total
                    for t, x in idx[p]:
                        totals[t] += d * x
                touched += len(idx[p])
            newest[m] = v["ns"]
            if seq == 1:
                continue                                    # the line-up is not a live ball
            age = now - v["ns"]
            if caught[1]:
                lat.add(age / 1e6)
                st["live"] += 1
                st["updates"] += touched
            else:
                st["replayed"] += 1
                st["rebuilt"] += 1
                st["max_age"] = max(st["max_age"], age / 1e6)
        for m, ns in newest.items():
            prev = shown.get(m)
            if prev is not None and caught[1]:
                st["shown"] += 1
                if ns < prev[1]:
                    st["old"] += 1                          # this ball existed before the previous one was pushed
            shown[m] = (ns, now)

    def recount_off(k=200):
        off = 0
        for t in random.sample(range(a.teams), min(k, a.teams)):
            pts = points[team_match[t]]
            off += sum(pts[p] * x for p, x in team_picks[t]) != totals[t]
        return off

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        while True:
            ms = await c.poll_messages(STREAM, LANE, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else PollingStrategy.First())
            if ms:
                apply(ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                                # more is waiting (the rebuild): read on at once
            if pid not in head:
                head.add(pid)                               # this partition's replay reached the head of the log
                if len(head) == PARTITIONS:
                    caught[0] = time.monotonic()
                    live = [m for m in matches if m in seen]
                    started_teams = sum(1 for t in totals if t)
                    print(f"[{a.name}] rebuilt from the log in {caught[0] - started:.2f}s: points for {len(live)} of "
                          f"{len(matches)} live matches, {st['rebuilt']:,} balls replayed, totals off {recount_off(1000)}; "
                          f"without the log {started_teams:,} of {a.teams:,} teams "
                          f"({100 * started_teams / a.teams:.0f}%) would show the wrong total", flush=True)
            await asyncio.sleep(a.poll_ms / 1000)           # the feed writes once per --tick-ms; wait for the next write

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught[1]:
                # Live once the log is replayed and a whole second of balls arrived inside the budget;
                # partitions interleave, so one fresh ball alone doesn't mean the replay is finished.
                if caught[0] is not None and st["replayed"] and st["max_age"] < a.budget_ms:
                    caught[1] = True
                print(f"[{a.name}] catching up: {st['replayed']:,} balls replayed this second, "
                      f"{len(head)} of {PARTITIONS} partitions at the head of the log", flush=True)
                st.update(replayed=0, max_age=0.0)
                lat.flush()
                continue
            r, n = lat.flush(), st["live"]
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            old = 100 * st["old"] / st["shown"] if st["shown"] else 0.0
            print(f"[{a.name}] {n:>5,} balls/s  ball-to-node p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  team totals updated {st['updates']:,}/s  "
                  f"pushed after a newer ball existed {old:.3f}%  gaps {st['gaps']}  totals off {recount_off()}", flush=True)
            st.update(live=0, shown=0, old=0, updates=0)

    await asyncio.gather(report(), *(read(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="points-1")
    p.add_argument("--matches", type=int, default=40, help="same as scoring.py")
    p.add_argument("--teams", type=int, default=200_000, help="user teams held by this node")
    p.add_argument("--budget-ms", type=float, default=20, help="p99 budget for ball-to-node")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
