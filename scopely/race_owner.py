"""A race owner: holds every race on the partitions it owns, fed by the race lane.

Run one per --worker (0 .. --workers-1); worker w owns the race-rolls partitions p with p % workers == w, so
every race has exactly one owner and its rolls arrive in one order, with no lock and no conditional write.
Reads its partitions directly (no consumer group: a group reader visits one partition per poll, so a roll waited
for its partition's turn and the tail grew on a busy host), on --readers connections of its own, from the start
of the log, then from its own position; nothing is stored on the broker. A restarted owner rebuilds every race
it owns from the log.

For each roll it runs common.Race.apply (the race rules, the same code the truth check runs) and publishes, keyed
by race, to race-updates, which every game server reads (game_servers.py):
  * lap: a team completed a lap (lap rewards);
  * finish: a team crossed the line, with its place (1st, 2nd, 3rd);
  * end: three teams are home, the last takes 4th, and the next race on this track starts.
A roll the client sent twice (a retry after a throttled write), a roll for a race that already ended and a roll
from a team that already finished change nothing; each is counted.

Prints roll-to-race-owner p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, PARTITIONS, ROLLS, STREAM, UPDATES, Race, Window, ensure_topics, load, msg, part


async def main(a):
    name = f"owner-{a.worker}"

    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ROLLS, UPDATES)
    out = await (await own_client()).producer(STREAM, UPDATES, topic_partitions_count=PARTITIONS[UPDATES])
    lat, stats = Window(), defaultdict(float)
    races = defaultdict(Race)               # race -> its state, rebuilt from the log on start
    pending = defaultdict(list)             # race-updates partition -> updates waiting for the next send
    useq = defaultdict(int)                 # race -> update sequence (readers keep the newest)
    caught_up = [False]                     # a new owner first replays rolls from before it started
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_roll(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest roll seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        r = d["r"]
        outcome, events = races[r].apply(d, a.track, a.lap)
        stats[outcome] += 1
        for kind, tm, v in events:
            stats[kind] += 1
            useq[r] += 1
            pending[part(r, PARTITIONS[UPDATES])].append(SendMessage(msg(
                r=r, rn=d["rn"], u=useq[r], kind=kind, tm=tm, v=v, rns=d["ns"])))

    async def read(pids):
        # Each partition in turn, from the start of the log, then from our own position. A full batch means
        # more is waiting, so poll again at once.
        c = await own_client()
        who = {p: Consumer.Single(f"{name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, ROLLS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_roll(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_updates():
        # Updates are small and come in bursts; one batch per partition every 2 ms, sent together, keeps them
        # off a send-per-update loop and keeps each race's updates in order.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batches = dict(pending)
                    pending.clear()
                    await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                    stats["sent"] += sum(len(ms) for ms in batches.values())

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of rolls arrived inside the budget; partitions interleave,
                # so one fresh roll alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{name}] catching up: replayed {int(stats['replayed']):,} earlier rolls this second, "
                      f"{len(races):,} races rebuilt", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{name}] {r['n']:>6,} rolls/s  roll-to-race-owner p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  "
                  f"p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms}ms)  laps {int(stats['lap']):,}  "
                  f"finishes {int(stats['finish']):,}  races ended {int(stats['end']):,}  "
                  f"retries counted once {int(stats['retry']):,}  rolls after the team finished "
                  f"{int(stats['finished']):,}  rolls into an ended race {int(stats['ended']):,}  "
                  f"updates sent {int(stats['sent']):,}  races held {len(races):,}", flush=True)
            stats.clear()

    parts = [p for p in range(PARTITIONS[ROLLS]) if p % a.workers == a.worker]
    await asyncio.gather(send_updates(), report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--worker", type=int, default=0)
    p.add_argument("--workers", type=int, default=1, help="race owners sharing the lane's partitions")
    p.add_argument("--track", type=int, default=1200, help="points from start to finish (same in game_servers.py)")
    p.add_argument("--lap", type=int, default=300, help="points per lap (same in game_servers.py)")
    p.add_argument("--budget-ms", type=int, default=50, help="roll-to-race-owner p99 budget for the lane")
    p.add_argument("--readers", type=int, default=1, help="connections reading this owner's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
