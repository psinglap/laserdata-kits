"""Game server hosts: every shot onto the lane, and every match termination applied before the cheater's next shot
lands.

Plays --hosts game server hosts in one process, each running --matches-per-host matches of 10 players. Every shot
a player fires is one record on combat-actions, keyed by match: host, match, player, weapon, reaction time (enemy
first visible to the shot), snap (crosshair travel in the last 50 ms before the shot), hit, headshot and a
per-match sequence. One batch per partition per 10 ms tick, so the lane costs the server one round trip a tick,
never one per shot. Each host also reads match-terminations directly on its own connection and ends its own
matches the moment a termination arrives: no result is recorded, so nobody loses rating to that cheater. A host
never sees another host's matches; a termination reaches its host through the log.

The same process plays the players, on fixed schedules (all illustrative):
  * every match's own shots: --actions-per-s spread over all matches, human reaction times and snaps;
  * an AIMBOT every --cheat-every s: one player in a match not yet halfway through switches it on and fires
    --cheat-rps shots a second, inhumanly fast and on target, for --cheat-for s (and keeps firing once cut off);
  * matches end after --match-s with a result, and a new match takes the slot.

The business numbers: cheater shots that landed after the match's termination was published, because it had not
reached the host yet; the same for a lane --compare-ms behind; and cheater matches that still ended with a result.
Printed every second, with termination-to-host p50/p99: the second hop of the loop.
"""
import argparse
import asyncio
import gc
import os
import random
import statistics
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ACTIONS, DEFAULT_URL, PARTITIONS, STREAM, TERMINATIONS, WEAPONS, Window, ensure_topics, load,
                    match_id, msg, part, player_id)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ACTIONS, TERMINATIONS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, ACTIONS, topic_partitions_count=PARTITIONS[ACTIONS],
                                                   mode=cfg)
    start_ns = time.time_ns()
    run = f"{random.getrandbits(16):04x}"   # match IDs never repeat across runs of the kit
    hosts = list(range(1, a.hosts + 1))
    counter = defaultdict(int)
    next_player = [0]
    live = {}                               # match -> {"host", "players", "end", "seq"}
    slots = []                              # one entry per match slot: the match it is running now
    cheats = {}                             # match -> (player, start ns, end ns)
    truth = {}                              # match -> termination ns, once its host has it
    recent = defaultdict(lambda: deque(maxlen=2048))   # cheat match -> (ns, landed) of shots before that is known
    first_cheat = {}                        # match -> ns of the aimbot's first shot
    detect, lat, stats = [], Window(), defaultdict(float)
    cmp_ns = a.compare_ms * 1_000_000

    def new_match(host, age_s=0.0):
        counter[host] += 1
        m = match_id(f"{a.shard}{run}", host, counter[host])
        players = [player_id(next_player[0] + i) for i in range(10)]
        next_player[0] += 10
        live[m] = {"host": host, "players": players, "end": time.monotonic() + a.match_s - age_s, "seq": 0,
                   "start": time.monotonic() - age_s}
        return m

    for h in hosts:
        for _ in range(a.matches_per_host):
            slots.append(new_match(h, random.uniform(0, a.match_s)))   # staggered, as a live fleet is

    def end(slot_i, why):
        m = slots[slot_i]
        info = live.pop(m)
        if why == "result":
            stats["results"] += 1
            if m in cheats and cheats[m][1] <= time.time_ns():
                stats["with_result"] += 1   # a cheater's match counted for rating: rollback territory
        slots[slot_i] = new_match(info["host"])

    def count(m, t, landed):
        """One aimbot shot in a cheat match, counted against its termination once that is known."""
        d = truth.get(m)
        if d is None:
            recent[m].append((t, landed))
            return
        if t <= d:
            return
        if t - d < cmp_ns:
            stats["would"] += 1             # a lane --compare-ms behind would have let it land
        if landed:
            stats["after"] += 1             # landed after the termination was published

    def reader(h):
        async def run_reader():
            c = await own_client()
            who, nxt = Consumer.Single(f"gs{h:02d}-{os.getpid()}"), None
            while True:
                ms = await c.poll_messages(STREAM, TERMINATIONS, consumer=who, partition_id=0, count=1000,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                           else PollingStrategy.First())
                if not ms:
                    await asyncio.sleep(a.poll_ms / 1000)
                    continue
                nxt = ms[-1].offset() + 1
                for msg_ in ms:
                    d = load(msg_.payload())
                    if d["ns"] <= start_ns or d.get("h") != h:
                        continue            # replayed at start-up, or another host's match
                    m = d["m"]
                    lat.add((time.time_ns() - d["ns"]) / 1e6)
                    if m in truth:
                        continue            # repeated termination: already applied
                    truth[m] = d["ns"]
                    stats["terminated"] += 1
                    if m not in cheats:
                        stats["unplanted"] += 1     # a match nobody cheated in was ended: a false termination
                    else:
                        detect.append((d["ns"] - first_cheat.get(m, d["ns"])) / 1e6)
                    if m in live:
                        end(slots.index(m), "terminated")
                    else:
                        stats["late"] += 1  # the match had already ended with its result
                    for t, landed in recent.pop(m, ()):
                        count(m, t, landed)
        return run_reader

    async def aimbots():
        # One aimbot switched on every --cheat-every s, from its due time, in a match not yet halfway through.
        due = time.monotonic() + a.first_at
        while True:
            now = time.monotonic()
            if now >= due:
                m = random.choice(slots)
                info = live[m]
                if m not in cheats and now - info["start"] < a.match_s / 2:
                    st = time.time_ns()
                    cheats[m] = (random.choice(info["players"]), st, st + int(a.cheat_for * 1e9))
                    stats["cheaters"] += 1
                    due += a.cheat_every
            await asyncio.sleep(0.02)

    def shot(m, p, rt, snap, hit, batches):
        info = live[m]
        info["seq"] += 1
        batches[part(m, PARTITIONS[ACTIONS])].append(SendMessage(msg(
            h=info["host"], m=m, p=p, w=random.choice(WEAPONS), rt=round(rt, 1), snap=round(snap, 1),
            hit=hit, hs=hit and random.random() < 0.25, seq=info["seq"])))
        stats["shots"] += 1

    async def shot_loop():
        t0 = due = time.monotonic()
        owed, cheat_owed = 0.0, defaultdict(float)
        async with producer:
            while True:
                t = time.monotonic() - t0
                k = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.actions_per_s * k / 100
                batches = defaultdict(list)
                for _ in range(int(owed)):
                    m = random.choice(slots)
                    p = random.choice(live[m]["players"])
                    if m in cheats and p == cheats[m][0]:
                        continue            # the cheater's shots come from the aimbot below
                    shot(m, p, max(90.0, random.gauss(280, 70)), random.expovariate(1 / 8),
                         random.random() < 0.3, batches)
                owed -= int(owed)
                now_ns = time.time_ns()
                for m, (p, st, en) in list(cheats.items()):
                    if now_ns >= en:
                        del cheats[m]
                        cheat_owed.pop(m, None)
                        recent.pop(m, None)
                        first_cheat.pop(m, None)
                        continue
                    if now_ns < st:
                        continue
                    cheat_owed[m] += a.cheat_rps / 100
                    for _ in range(int(cheat_owed[m])):
                        landed = m in live  # once the match is over, nothing the aimbot fires lands
                        count(m, time.time_ns(), landed)
                        stats["aimbot"] += 1
                        if not landed:
                            stats["refused"] += 1
                            continue
                        first_cheat.setdefault(m, time.time_ns())
                        shot(m, p, random.uniform(60, 130), random.uniform(30, 90), True, batches)
                    cheat_owed[m] -= int(cheat_owed[m])
                now = time.monotonic()
                for i, m in enumerate(slots):
                    if live[m]["end"] <= now:
                        end(i, "result")
                # One batch per partition, all sent at once: per-match order is kept, about one round trip a tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                stats["peak"] = k > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        print(f"[servers] {a.hosts} game server hosts x {a.matches_per_host} matches, shard {a.shard}, "
              f"an aimbot every {a.cheat_every:g} s at {a.cheat_rps:g} shots/s, matches of {a.match_s:g} s",
              flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            win = f"{statistics.median(detect):.0f}ms" if detect else "-"
            print(f"[servers] {int(stats['shots']):>7,} shots/s ({phase})  aimbot shots {int(stats['aimbot']):,}  "
                  f"refused {int(stats['refused']):,}  terminations {int(stats['terminated'])} "
                  f"(aimbot caught in {win} median)  "
                  f"cheater shots landed after the termination {int(stats['after']):,} "
                  f"(at {a.compare_ms:g} ms it would be {int(stats['would']):,})  "
                  f"cheater matches with a result {int(stats['with_result'])}  "
                  f"false terminations {int(stats['unplanted'])}  "
                  f"termination-to-host n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("shots", "aimbot", "refused", "terminated", "after", "would", "with_result", "unplanted",
                      "results", "late"):
                stats[k] = 0
            del detect[:-200]

    readers = [reader(h)() for h in hosts]
    gc.freeze()                             # keep start-up objects out of the collector's way
    await asyncio.gather(shot_loop(), report(), aimbots(), *readers)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--shard", default="na", help="the shard (region) this lane serves")
    p.add_argument("--hosts", type=int, default=8, help="game server hosts, each with its own termination reader")
    p.add_argument("--matches-per-host", type=int, default=108, help="matches per host (3 per core on 36 cores)")
    p.add_argument("--actions-per-s", type=float, default=10_000, help="shots per second across all matches")
    p.add_argument("--match-s", type=float, default=120, help="match length in seconds (compressed; illustrative)")
    p.add_argument("--cheat-every", type=float, default=2, help="seconds between aimbots switched on")
    p.add_argument("--cheat-rps", type=float, default=8, help="aimbot shots per second")
    p.add_argument("--cheat-for", type=float, default=10, help="seconds an aimbot keeps firing")
    p.add_argument("--first-at", type=float, default=5, help="seconds before the first aimbot")
    p.add_argument("--compare-ms", type=float, default=1000, help="also count what a lane this far behind lets land")
    p.add_argument("--poll-ms", type=float, default=1, help="termination poll wait when nothing is waiting")
    p.add_argument("--peak", type=float, default=1, help="shot-rate multiplier during the peak (evening queue)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
