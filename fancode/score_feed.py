"""The score feed: every score update of every live match, in every sport, written once to the scorecard lane.

One writer per partition, each with its own connection, writing once per --tick-ms with the updates that came
in since the last write, so every match's updates stay in order. A message is one update: the match's own key
(the `key` scorecardPoll returns, one more on every update), what happened, and the resulting score of both
sides (the score so far, never a delta), so a node can answer a poll from the newest message alone.

On start it writes one opening message per match (key 1, the score at the start). Match time is compressed:
--speed 15 (the default) moves every score 15 times faster than real time (a cricket ball every ~2.3 s instead
of ~35 s) so every second has updates to time; --speed 1 is real time. Scores are illustrative.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, SPORTS, STREAM, Window, connect, ensure_topic, match_ids, pack, \
    partition_of, sport_of

BALLS = [("0", 0, 35), ("1", 1, 33), ("2", 2, 9), ("4", 4, 12), ("SIX", 6, 5), ("W", 0, 5), ("WD", 1, 1)]


class Match:
    def __init__(self, mid: str, sport: str):
        self.id, self.sport, self.key, self.due = mid, sport, 0, 0.0
        self.bat = 0
        if sport == "cricket":
            self.sc = [[0, 0, 0], [0, 0, 0]]                    # runs, wickets, legal balls per innings
        elif sport == "tennis":
            self.sc = [[0, 0, 0], [0, 0, 0]]                    # sets, games, points (0, 15, 30, 40)
        else:
            self.sc = [0, 0]                                    # points or goals

    def opening(self) -> bytes:
        self.key += 1
        return pack(self.id, self.sport, self.key, "START", self.sc)

    def step(self, rng) -> str:
        s, sc = self.sport, self.sc
        side = rng.randrange(2)
        if s == "cricket":
            label, runs, _ = rng.choices(BALLS, [w for *_, w in BALLS])[0]
            inn = sc[self.bat]
            inn[0] += runs
            if label != "WD":
                inn[2] += 1
            if label == "W":
                inn[1] += 1
            if inn[2] >= 120 or inn[1] >= 10:
                self.bat = 1 - self.bat
                if self.bat == 0:                                # both innings done: a new match on the same id
                    self.sc = [[0, 0, 0], [0, 0, 0]]
            return f"{inn[2] // 6}.{inn[2] % 6} {label}"
        if s == "tennis":
            me, op = sc[side], sc[1 - side]
            me[2] += 1
            if me[2] >= 4 and me[2] - op[2] >= 2:
                me[1], me[2], op[2] = me[1] + 1, 0, 0
                if me[1] >= 6 and me[1] - op[1] >= 2:
                    me[0], me[1], op[1] = me[0] + 1, 0, 0
                return "GAME"
            return "POINT"
        if s == "kabaddi":
            label, pts = rng.choice([("RAID +1", 1), ("RAID +2", 2), ("TACKLE +1", 1), ("EMPTY RAID", 0), ("ALL OUT +2", 2)])
        elif s == "basketball":
            label, pts = rng.choice([("+2", 2), ("+2", 2), ("+3", 3), ("FT", 1), ("MISS", 0)])
        else:                                                   # football, hockey: most updates move the clock, not the score
            label, pts = rng.choices([("GOAL", 1), ("SHOT", 0), ("CARD", 0), ("CORNER", 0), ("SAVE", 0)], [1, 6, 2, 4, 4])[0]
        sc[side] += pts
        return label

    def update(self, rng, waited_ns: int) -> bytes:
        event = self.step(rng)
        self.key += 1
        return pack(self.id, self.sport, self.key, event, self.sc, waited_ns)


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, LANE, PARTITIONS)
    matches = [Match(m, sport_of(i)) for i, m in enumerate(match_ids(a.matches))]
    by_part = [[m for m in matches if partition_of(m.id) == p] for p in range(PARTITIONS)]
    rng = random.Random(7)
    acks, sent, t0 = Window(), [0], time.monotonic()
    ready, started = [0], asyncio.Event()

    def gap(m):
        return SPORTS[m.sport] / a.speed * rng.uniform(0.6, 1.4)

    async def writer(pid):
        c = await connect(a.url)
        mine = by_part[pid]
        if mine:
            await c.send_messages(STREAM, LANE, pid, [SendMessage(m.opening()) for m in mine])
        ready[0] += 1
        await started.wait()
        for m in mine:                                          # matches start spread over one update interval
            m.due = t0 + rng.random() * SPORTS[m.sport] / a.speed
        due = time.monotonic()
        while True:
            now = time.monotonic()
            batch = []
            for m in mine:
                while m.due <= now:
                    # An update arrives at m.due and waits for this write; that wait counts as latency.
                    batch.append(SendMessage(m.update(rng, int((now - m.due) * 1e9))))
                    m.due += gap(m)
            if batch:
                s0 = time.perf_counter()
                await c.send_messages(STREAM, LANE, pid, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
                sent[0] += len(batch)
            due += a.tick_ms / 1000                             # from the due time, so a slow tick doesn't drift
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def go():
        nonlocal t0
        while ready[0] < PARTITIONS:
            await asyncio.sleep(0.05)
        sports = {}
        for m in matches:
            sports[m.sport] = sports.get(m.sport, 0) + 1
        print(f"[score-feed] opening scores: {len(matches)} live matches ("
              + ", ".join(f"{n} {s}" for s, n in sports.items()) + f") across {PARTITIONS} partitions", flush=True)
        gc.freeze()                                             # keep startup objects out of the collector's way
        t0 = time.monotonic()
        started.set()

    async def report():
        await started.wait()
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[score-feed] {sent[0]:>5,} updates/s  {r['n']:,} writes  write ack p50 {r['p50']:.1f}ms  "
                  f"p99 {r['p99']:.1f}ms", flush=True)
            sent[0] = 0

    await asyncio.gather(go(), report(), *(writer(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--matches", type=int, default=40, help="live matches at once, all sports")
    p.add_argument("--speed", type=float, default=15, help="match time vs real time (1 = real time)")
    p.add_argument("--tick-ms", type=float, default=2, help="each writer writes once per this many ms")
    asyncio.run(main(p.parse_args()))
