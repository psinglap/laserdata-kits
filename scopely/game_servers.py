"""A fleet of MONOPOLY GO! game servers during a team race: every dice roll onto the lane, and every lap, finish
and race end applied to every server's race view.

Plays --servers game servers and the players in one process. Players are in races of 4 teams of 4 (your team
against three others). When the race starts, players arrive over --ramp-s seconds, from zero to the full rate,
the way the race mini game starts at a set time. Each player taps the dice popper at illustrative gaps
(common.next_gap), and each roll lands on any game server, as a load balancer would send it. For every roll
the server publishes to race-rolls, keyed by race:
  * race, race number, team, player, server, the player's roll sequence and the player's points in this race
    (the resulting value), so a retried roll counts once.
A share of rolls (--retry-rate) is sent twice, the way a client retries after a throttled write.

Each game server also reads race-updates on its own connection, every partition directly from the start of the
log, so a server added during the event builds its race view from the log. A finish tells the team to stop
rolling; an end starts the next race on that track.

The business number: of the players whose team just finished (or whose race just ended), how many rolled again
before their game server had the news, so they spent dice on a race that was already decided. Prints it every
second, with roll-to-every-game-server p50/p99 (both hops of the loop). It also runs the race rules itself on
every roll it sends (common.Race, in send order) and checks every place the race owners announce against it.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, MEAN_GAP, PARTITIONS, PLAYERS, ROLLS, STREAM, TEAMS, UPDATES, Race, Window,
                    ensure_topics, load, msg, next_gap, part, player, race, roll_points)

PER_RACE = TEAMS * PLAYERS


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ROLLS, UPDATES)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, ROLLS, topic_partitions_count=PARTITIONS[ROLLS], mode=cfg)
    start_ns = time.time_ns()
    servers = [f"game-{i:03d}" for i in range(a.servers)]
    n_races = max(1, round(a.rate * MEAN_GAP / PER_RACE))
    n_players = n_races * PER_RACE
    rids = [race(i) for i in range(n_races)]
    pid = [part(r, PARTITIONS[ROLLS]) for r in rids]
    pnames = [player(i // PER_RACE, (i % PER_RACE) // PLAYERS, i % PLAYERS) for i in range(n_players)]
    truth = [Race() for _ in range(n_races)]  # the race rules on every roll, in send order (= log order)
    places = {}                               # (race, race number, team) -> place the rules gave
    p_rn = [1] * n_players                    # player -> race number their points are for
    p_tot = [0] * n_players                   # player -> points in that race
    p_seq = [0] * n_players
    news = {}                                 # player -> (race number, ns): their team finished or race ended
    view = {s: {} for s in servers}           # game server -> {race index: [race number, finished teams, update seq]}
    now = time.monotonic()
    due = [(now + random.uniform(0, a.ramp_s), i) for i in range(n_players)]
    heapq.heapify(due)
    retry = defaultdict(list)                 # partition -> roll bodies the client sends again next tick
    lat, stats = Window(), defaultdict(float)
    total = defaultdict(int)
    rindex = {r: i for i, r in enumerate(rids)}

    def seen(s, ri):
        v = view[s].get(ri)
        if v is None:
            v = view[s][ri] = [1, set(), 0]
        return v

    async def server_reader(s):
        c = await own_client()
        pids = list(range(PARTITIONS[UPDATES]))
        who = {p: Consumer.Single(f"{s}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, UPDATES, consumer=who[p], partition_id=p, count=1000,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if not ms:
                    continue
                nxt[p] = ms[-1].offset() + 1
                t = time.time_ns()
                for m in ms:
                    d = load(m.payload())
                    ri = rindex.get(d["r"])
                    if ri is None or d["rns"] < start_ns:
                        continue            # from an earlier run on the same broker
                    v = seen(s, ri)
                    if d["u"] <= v[2]:
                        continue            # older or repeated update: the newest wins
                    v[2] = d["u"]
                    lat.add((t - d["rns"]) / 1e6)
                    if d["kind"] == "finish":
                        if places.get((ri, d["rn"], d["tm"])) != d["v"]:
                            total["mismatch"] += 1
                        if d["rn"] == v[0]:
                            v[1].add(d["tm"])
                    elif d["kind"] == "end" and d["rn"] >= v[0]:
                        v[0], v[1] = d["rn"] + 1, set()
                more = more or len(ms) == 1000
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def tell(i0, i1, rn, t):
        for j in range(i0, i1):
            news.setdefault(j, (rn, t))

    async def roll_loop():
        tick = time.monotonic()
        async with producer:
            while True:
                now = time.monotonic()
                batches = {p: [SendMessage(b) for b in bs] for p, bs in retry.items()}
                retry.clear()
                stats["retried"] += sum(len(v) for v in batches.values())
                while due and due[0][0] <= now:
                    _, i = heapq.heappop(due)
                    heapq.heappush(due, (now + next_gap(), i))
                    ri, tm = i // PER_RACE, (i % PER_RACE) // PLAYERS
                    s = random.choice(servers)
                    v = seen(s, ri)
                    shown = tm in v[1]                  # this server says the team is home: wait for the next race
                    if i in news:                       # first roll attempt since the team finished or race ended
                        rn, t = news.pop(i)
                        stats["after"] += 1
                        if not (v[0] > rn or (v[0] == rn and shown)):
                            stats["early"] += 1         # rolled before this server had the news
                        if (time.time_ns() - t) / 1e6 < a.compare_ms:
                            stats["early_at_compare"] += 1
                    if shown:
                        stats["held"] += 1
                        continue
                    if p_rn[i] != v[0]:
                        p_rn[i], p_tot[i] = v[0], 0
                    p_tot[i] += roll_points()
                    p_seq[i] += 1
                    d = {"r": rids[ri], "rn": v[0], "tm": tm, "p": pnames[i], "s": s, "seq": p_seq[i],
                         "tot": p_tot[i], "ns": time.time_ns()}
                    body = msg(**d)
                    batches.setdefault(pid[ri], []).append(SendMessage(body))
                    if random.random() < a.retry_rate:
                        retry[pid[ri]].append(body)     # the same roll again, sent with the next tick
                    stats["rolls"] += 1
                    _, events = truth[ri].apply(d, a.track, a.lap)
                    for kind, team, val in events:
                        base = ri * PER_RACE
                        if kind == "finish":
                            places[(ri, d["rn"], team)] = val
                            stats["finish"] += 1
                            tell(base + team * PLAYERS, base + (team + 1) * PLAYERS, d["rn"], d["ns"])
                        elif kind == "end":
                            stats["end"] += 1
                            tell(base, base + PER_RACE, d["rn"], d["ns"])
                # One batch per partition, all sent at once: each race's rolls keep their order, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[game] {a.servers} game servers, {n_races:,} races, {n_players:,} players, track {a.track} points, "
              f"lap {a.lap}, ramp {a.ramp_s:g} s", flush=True)
        t0 = time.monotonic()
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            for k in ("after", "early", "early_at_compare"):
                total[k] += stats[k]
            phase = "ramp" if time.monotonic() - t0 < a.ramp_s else "race on"
            print(f"[game] {int(stats['rolls']):>6,} rolls/s ({phase})  retries sent {int(stats['retried']):,}  "
                  f"finishes {int(stats['finish']):,}  races ended {int(stats['end']):,}  "
                  f"first rolls after a finish or race end {int(stats['after']):,}, before the server had it "
                  f"{int(stats['early']):,} (at {a.compare_ms:g} ms it would be {int(stats['early_at_compare']):,})  "
                  f"held {int(stats['held']):,}  places checked wrong {total['mismatch']}  "
                  f"roll-to-every-game-server n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            stats.clear()

    gc.freeze()                             # keep start-up objects out of the collector's way
    await asyncio.gather(roll_loop(), report(), *(server_reader(s) for s in servers))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--servers", type=int, default=16, help="game servers, each with its own race-updates reader")
    p.add_argument("--rate", type=float, default=20_000, help="dice rolls per second at full rate, all servers")
    p.add_argument("--ramp-s", type=float, default=300, help="seconds from race start to the full rate (five minutes)")
    p.add_argument("--track", type=int, default=1200, help="points from start to finish (same in race_owner.py)")
    p.add_argument("--lap", type=int, default=300, help="points per lap (same in race_owner.py)")
    p.add_argument("--retry-rate", type=float, default=0.005, help="share of rolls the client sends twice")
    p.add_argument("--compare-ms", type=float, default=2000,
                   help="also count rolls that would beat a race view refreshed this slowly")
    p.add_argument("--poll-ms", type=float, default=1, help="race-updates readers: wait when nothing is waiting")
    asyncio.run(main(p.parse_args()))
