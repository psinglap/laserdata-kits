"""The anti-cheat scorer, fed by the combat-action lane.

Reads every partition of combat-actions directly (no consumer group: a group reader visits one partition per poll,
so with 16 partitions a shot waited for its partition's turn), on --readers connections of its own, tracking its
own position. Each record is one shot a game server recorded: host, match, player, weapon, reaction time, snap,
hit, headshot and a per-match sequence. A replayed or repeated record is recognised by its sequence and changes
nothing.

It terminates a match when one player's last --window hits look like an aimbot: at least --min-flags of them came
faster than --max-rt-ms after the enemy was first visible and after a crosshair snap wider than --min-snap
degrees. suspicious() is a stand-in for your models; put yours there. Each termination goes out on
match-terminations, which every game server host reads (game_servers.py), with the match's own sequence, the host
that runs it and the evidence, so a restarted scorer or host rebuilds it from the log.

Prints action-to-scorer p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the terminations sent.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import ACTIONS, DEFAULT_URL, PARTITIONS, STREAM, TERMINATIONS, Window, ensure_topics, load, msg


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ACTIONS, TERMINATIONS)
    out = await (await own_client()).producer(STREAM, TERMINATIONS, topic_partitions_count=PARTITIONS[TERMINATIONS])
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # match -> last sequence applied
    hits = defaultdict(lambda: deque(maxlen=a.window))   # (match, player) -> (reaction ms, snap) of recent hits
    terminated = set()
    pending = []                            # terminations waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays shots from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def suspicious(hs) -> int | None:
        """Stands in for your models. Here: most recent hits inhumanly fast after a wide snap."""
        if len(hs) < a.window:
            return None
        flags = sum(1 for rt, snap in hs if rt < a.max_rt_ms and snap > a.min_snap)
        return flags if flags >= a.min_flags else None

    def on_shot(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest shot seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        mt = d["m"]
        if d["seq"] <= seq.get(mt, 0):
            stats["dropped"] += 1           # older or repeated record: already counted
            return
        seq[mt] = d["seq"]
        stats["shots"] += 1
        if not d["hit"] or mt in terminated:
            return
        key = (mt, d["p"])
        hs = hits[key]
        hs.append((d["rt"], d["snap"]))
        flags = suspicious(hs)
        if flags is None:
            return
        terminated.add(mt)
        stats["terminated"] += 1
        if age > a.replay_terminate_s * 1000:
            return                          # replayed from long ago: its termination is already in the log
        pending.append(SendMessage(msg(h=d["h"], m=mt, p=d["p"], seq=1, state="terminated",
                                       reason="aim anomaly", flags=flags, of=a.window)))

    async def read(pids):
        # Each partition in turn, from the start of the log (the windows are rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, ACTIONS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_shot(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_terminations():
        # Terminations are small and rare; one batch every 2 ms keeps them off a send-per-decision loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def forget():
        # Matches end; drop windows of matches with no shot for a while so memory stays flat.
        last = {}
        while True:
            await asyncio.sleep(10)
            now = dict(seq)
            for k in [k for k in hits if last.get(k[0]) == now.get(k[0])]:
                del hits[k]
            last = now

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of shots arrived inside the budget; partitions interleave, so one
                # fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier shots this second, "
                      f"{len(terminated):,} matches terminated so far", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} shots/s  action-to-scorer p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"terminations sent {int(stats['sent']):,}  matches terminated {len(terminated):,}", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[ACTIONS]))
    await asyncio.gather(send_terminations(), report(), forget(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="anticheat")
    p.add_argument("--budget-ms", type=int, default=50,
                   help="action-to-scorer p99 budget for the lane, well inside the gap between two aimbot shots")
    p.add_argument("--readers", type=int, default=2, help="connections reading the combat-action partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--window", type=int, default=8, help="recent hits per player that the model looks at")
    p.add_argument("--min-flags", type=int, default=6, help="flagged hits inside the window to terminate")
    p.add_argument("--max-rt-ms", type=float, default=140, help="reaction time below which a hit is flagged")
    p.add_argument("--min-snap", type=float, default=25, help="snap in degrees above which a hit is flagged")
    p.add_argument("--replay-terminate-s", type=float, default=30,
                   help="after a restart, still publish terminations for evidence younger than this")
    asyncio.run(main(p.parse_args()))
