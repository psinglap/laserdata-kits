"""The source of truth: players, the games they play, every result, and every tap on Play again.

Each player cycles: tap Play again in a lobby, get matched, play one game, see the result, think for a few
seconds, tap Play again. When a game ends, the game server publishes the player's resulting form to the
player-form topic, keyed by player, with a per-player sequence number: rating after the game, streak (positive
for wins in a row, negative for losses in a row) and games played, never only a delta.

Match requests go to match-requests, keyed by lobby. A matchmaker (matchmaker.py) decides from its own view of
the player's form: the rating to match at and, for a player on a losing streak of --protect-after games or more,
streak protection (an easier opponent, so the next game is more likely a win). The decision comes back on
matches. This script checks each decision against the player's true form at the moment of the tap:
  * matched on a stale form: the matchmaker's view did not have the player's last result yet;
  * streak missed: the player was on a losing streak and the matchmaker did not protect them;
  * protected after a win: the matchmaker still saw a losing streak the player had already broken.
Each decision also carries the decision the same matchmaker would have made from a view --stale-s seconds behind
(set on matchmaker.py), and that decision is checked against the same true form, before the game is played.

--surge adds players at --surge-at (arriving over --ramp-s) for --surge-for seconds, the shape of a live sporting
event's peak.
"""
import argparse
import asyncio
import gc
import heapq
import itertools
import math
import random
import time
from collections import defaultdict

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, FORM, GAMES, MATCHES, PARTITIONS, REQUESTS, STREAM, Window, connect,
                    ensure_topics, load, msg, part_of, player_id)


async def main(a):
    await ensure_topics(await connect(a.url), FORM, REQUESTS, MATCHES)
    lane, req_w, m_reader = await connect(a.url), await connect(a.url), await connect(a.url)   # one each

    rng = random.Random(7)
    # player -> [skill, rating, streak, games, seq, lobby, state]; skill is hidden, rating is what matchmaking sees
    pl = {}
    events = []                     # heap of (due monotonic, order, kind, player)
    order = itertools.count()
    outbox, reqs = [], []           # form changes and match requests waiting for the next tick's send
    pending = {}                    # request id -> (tap ns, player, seq, streak at the tap)
    ids = itertools.count(1)
    leaving = set()                 # players who leave at their next tap
    st, lat, run = defaultdict(int), Window(), {"surge": False, "added": False, "to_add": 0}

    def emit(p, kind):
        v = pl[p]
        v[4] += 1
        outbox.append((p, SendMessage(msg(k=p, seq=v[4], r=round(v[1], 1), streak=v[2], games=v[3],
                                          game=v[5], kind=kind))))

    def schedule(due, kind, p):
        heapq.heappush(events, (due, next(order), kind, p))

    def add_players(n, first_tap_within, after=0.0):
        base, now = len(pl), time.monotonic() + after
        for i in range(base, base + n):
            p = player_id(i)
            skill = rng.gauss(1200, 200)
            pl[p] = [skill, skill + rng.gauss(0, 120), 0, 0, 0, rng.choice(GAMES), "idle"]
            emit(p, "snapshot")             # seq 1: the form the player starts the session with, not timed
            schedule(now + rng.uniform(0, first_tap_within), "tap", p)
        return [player_id(i) for i in range(base, base + n)]

    async def send(writer, topic, items):
        out = defaultdict(list)             # one batch per partition: a key's messages stay in order in one partition
        for key, m in items:
            out[part_of(key, PARTITIONS[topic])].append(m)
        await asyncio.gather(*(writer.send_messages(STREAM, topic, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))

    async def flush_snapshot():
        items, outbox[:] = list(outbox), []
        for i in range(0, len(items), 5000):
            await send(lane, FORM, items[i:i + 5000])

    def tap(p, now_ns):
        v = pl[p]
        if p in leaving:
            v[6] = "gone"
            return
        rid = f"m{next(ids)}"
        pending[rid] = (now_ns, p, v[4], v[2])
        v[6] = "queued"
        reqs.append((v[5], SendMessage(msg(id=rid, p=p, lobby=v[5], ns=now_ns))))
        st["taps"] += 1

    def finish(p, opp):
        v = pl[p]
        expected = 1 / (1 + math.pow(10, (opp - v[1]) / 400))
        won = rng.random() < 1 / (1 + math.pow(10, (opp - v[0]) / 400))   # the outcome follows true skill
        v[1] += a.k_factor * ((1 if won else 0) - expected)
        v[2] = (max(v[2], 0) + 1) if won else (min(v[2], 0) - 1)
        v[3] += 1
        emit(p, "result")
        st["results"] += 1
        st["losses_3plus"] += v[2] <= -a.protect_after
        v[6] = "thinking"
        schedule(time.monotonic() + rng.uniform(a.think_min, a.think_max), "tap", p)

    async def run_players():
        add_players(a.players, a.think_max + a.game_max)
        await flush_snapshot()
        print(f"[players] snapshot: {a.players:,} players in {len(GAMES)} lobbies", flush=True)
        gc.freeze()                         # keep start-up tables out of the collector's way
        t0 = due = time.monotonic()
        surge_players = []
        while True:
            t = time.monotonic() - t0
            surge = a.surge > 1 and a.surge_at <= t < a.surge_at + a.surge_for
            if surge and not run["added"]:
                run["added"] = True
                run["to_add"] = int((a.surge - 1) * a.players)
                print(f"[players] live event: {run['to_add']:,} players join over {a.ramp_s:g} s, "
                      f"for {a.surge_for:g} s", flush=True)
            if surge and run["to_add"] > 0:
                # Players arrive over --ramp-s, a slice a tick; each one's first tap comes after its form is out.
                n = min(run["to_add"], max(1, int((a.surge - 1) * a.players / (a.ramp_s * 100))))
                run["to_add"] -= n
                surge_players += add_players(n, a.think_max, after=a.think_min)
            if run["added"] and not surge and surge_players:
                leaving.update(surge_players)   # the event ends: these players leave after their current game
                surge_players = []
            run["surge"] = surge
            now, now_ns = time.monotonic(), time.time_ns()
            while events and events[0][0] <= now:
                _, _, kind, p = heapq.heappop(events)
                if kind == "tap":
                    tap(p, now_ns)
                else:
                    finish(p, kind[1])
            changes, outbox[:] = list(outbox), []
            taps, reqs[:] = list(reqs), []
            # The tick's results and taps go out together, each writer on its own connection.
            t_send = time.monotonic()
            await asyncio.gather(send(lane, FORM, changes), send(req_w, REQUESTS, taps))
            st["send_ms"] = max(st["send_ms"], 1000 * (time.monotonic() - t_send))
            st["late_ms"] = max(st["late_ms"], 1000 * (t_send - due))   # how late this tick started
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def read_matches():
        # Every partition read directly, from start-up on, on its own connection.
        who = {p: Consumer.Single(f"players-check-{p}") for p in range(PARTITIONS[MATCHES])}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)
        nxt = {}
        while True:
            more = False
            for p in who:
                ms = await m_reader.poll_messages(STREAM, MATCHES, consumer=who[p], partition_id=p, count=1000,
                                                  auto_commit=False,
                                                  polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_match(m)
                    more = more or len(ms) == 1000
            if not more:
                await asyncio.sleep(0.001)

    def judge(prefix, used_seq, protect, true_seq, true_streak):
        st[f"{prefix}_stale"] += used_seq < true_seq
        losing = true_streak <= -a.protect_after
        st[f"{prefix}_missed"] += losing and not protect
        st[f"{prefix}_wrong"] += protect and not losing

    def on_match(m):
        d = load(m.payload())
        req = pending.pop(d["id"], None)
        if req is None:
            return                          # answered twice (at-least-once) or already timed out
        tap_ns, p, true_seq, true_streak = req
        lat.add((time.time_ns() - tap_ns) / 1e6)
        st["matched"] += 1
        st["losing"] += true_streak <= -a.protect_after
        # Judge both decisions against the same true form, the one the player had at the tap, before the game.
        judge("live", d["seq"], d["protect"], true_seq, true_streak)
        judge("alt", d["alt_seq"], d["alt_protect"], true_seq, true_streak)
        pl[p][6] = "playing"
        schedule(time.monotonic() + rng.uniform(a.game_min, a.game_max), ("end", d["opp"]), p)

    async def report():
        while True:
            await asyncio.sleep(1)
            cutoff = time.time_ns() - 5_000_000_000
            for rid in [r for r, q in pending.items() if q[0] < cutoff]:
                _, p, _, _ = pending.pop(rid)
                st["unmatched"] += 1        # no match within 5 s: the player taps again
                pl[p][6] = "thinking"
                schedule(time.monotonic() + a.think_min, "tap", p)
            r = lat.flush()
            playing = sum(1 for v in pl.values() if v[6] != "gone")
            print(f"[players] {st['results']:>6,} results/s  {playing:,} players  taps {st['taps']:,} "
                  f"matched {st['matched']:,} ({'surge' if run['surge'] else 'steady'})  "
                  f"on a losing streak {st['losing']:,}  "
                  f"live view: stale form {st['live_stale']:,} streak missed {st['live_missed']:,} "
                  f"protected after a win {st['live_wrong']:,}  "
                  f"a view {a.stale_s:g}s behind: stale form {st['alt_stale']:,} streak missed {st['alt_missed']:,} "
                  f"protected after a win {st['alt_wrong']:,}  unmatched {st['unmatched']:,}  "
                  f"tap-to-match p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  "
                  f"tick late max {st['late_ms']:.0f}ms  send max {st['send_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(run_players(), read_matches(), report())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--players", type=int, default=100_000, help="players online at once")
    ap.add_argument("--game-min", type=float, default=30, help="shortest game, seconds")
    ap.add_argument("--game-max", type=float, default=90, help="longest game, seconds")
    ap.add_argument("--think-min", type=float, default=2, help="shortest time from a result to Play again, seconds")
    ap.add_argument("--think-max", type=float, default=8, help="longest time from a result to Play again, seconds")
    ap.add_argument("--protect-after", type=int, default=3, help="losses in a row that call for streak protection")
    ap.add_argument("--k-factor", type=float, default=24, help="rating change per game (Elo K)")
    ap.add_argument("--surge", type=float, default=1, help="players online multiplier during a live event (1 = off)")
    ap.add_argument("--surge-at", type=float, default=60, help="seconds after start")
    ap.add_argument("--surge-for", type=float, default=60)
    ap.add_argument("--ramp-s", type=float, default=5, help="seconds over which the surge's players arrive")
    ap.add_argument("--stale-s", type=float, default=10, help="the comparison view's delay, as set on matchmaker.py (label only)")
    asyncio.run(main(ap.parse_args()))
