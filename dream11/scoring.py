"""The scoring feed: every ball of every live match, written once to the live points lane.

One writer per partition, each with its own connection, writing once per --tick-ms with the
balls that came in since the last write, so every match's balls stay in order. A message is one
ball: the match's own sequence number, the over, what happened, and the resulting fantasy points
of every player that ball touched (totals so far, never a delta), so a reader can trust the
newest message for a player without having seen every one before it.

On start it writes one line-up message per match (sequence 1, every player on 0 points). Match
time is compressed: a real T20 ball comes every 30 to 40 seconds; --ball-s 2 (the default) bowls
one every 2 s per match so every second has balls to time. Use --ball-s 35 for real time.
Fantasy points here are illustrative (runs, boundary and six bonuses, wickets, catches), not
Dream11's points system.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, PLAYERS, STREAM, Window, connect, ensure_topic, match_ids, pack, partition_of

# outcome, runs, weight
OUTCOMES = [("0", 0, 35), ("1", 1, 33), ("2", 2, 9), ("3", 3, 1), ("4", 4, 12), ("SIX", 6, 5), ("W", 0, 5)]
LABELS, RUNS, WEIGHTS = zip(*[(o, r, w) for o, r, w in OUTCOMES])
RUN, FOUR_BONUS, SIX_BONUS, WICKET, CATCH = 1, 1, 2, 25, 8


class Match:
    def __init__(self, mid: str):
        self.id, self.seq, self.pts = mid, 0, [0] * PLAYERS
        self.bat, self.balls, self.wkts = 0, 0, 0          # side batting, legal balls and wickets this innings
        self.striker, self.non, self.next_in = 0, 1, 2      # batting order positions 0-10
        self.due = 0.0

    def lineup(self) -> bytes:
        self.seq += 1
        return pack(self.id, self.seq, "0.0", "XI", [[p, 0] for p in range(PLAYERS)])

    def ball(self, rng, waited_ns: int) -> bytes:
        over = f"{self.balls // 6}.{self.balls % 6 + 1}"
        k = rng.choices(range(len(LABELS)), WEIGHTS)[0]
        label, runs = LABELS[k], RUNS[k]
        bat, field = self.bat * 11, (1 - self.bat) * 11
        batter, bowler = bat + self.striker, field + 6 + (self.balls // 6) % 5   # five bowlers rotate by over
        touched = {}
        if label == "W":
            self.pts[bowler] += WICKET
            touched[bowler] = 1
            if rng.random() < 0.6:                              # caught
                fielder = field + rng.randrange(11)
                self.pts[fielder] += CATCH
                touched[fielder] = 1
            touched[batter] = 1                                 # out: total final, sent so the screen shows it
            self.wkts += 1
            self.striker, self.next_in = self.next_in, self.next_in + 1
        else:
            self.pts[batter] += runs * RUN + (FOUR_BONUS if runs == 4 else 0) + (SIX_BONUS if runs == 6 else 0)
            touched[batter] = 1
            touched[bowler] = 1                                 # bowler's figures move on every ball
            if runs % 2:
                self.striker, self.non = self.non, self.striker
        self.balls += 1
        if self.balls % 6 == 0:
            self.striker, self.non = self.non, self.striker     # end of the over
        if self.balls >= 120 or self.wkts >= 10 or self.next_in > 10:
            self.bat, self.balls, self.wkts, self.striker, self.non, self.next_in = 1 - self.bat, 0, 0, 0, 1, 2
        self.seq += 1
        return pack(self.id, self.seq, over, label, [[p, self.pts[p]] for p in touched], waited_ns)


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, LANE, PARTITIONS)
    matches = [Match(m) for m in match_ids(a.matches)]
    by_part = [[m for m in matches if partition_of(m.id) == p] for p in range(PARTITIONS)]
    rng = random.Random(11)
    acks, sent, t0 = Window(), [0], time.monotonic()
    ready, started = [0], asyncio.Event()

    async def writer(pid):
        nonlocal t0
        c = await connect(a.url)
        mine = by_part[pid]
        if mine:
            await c.send_messages(STREAM, LANE, pid, [SendMessage(m.lineup()) for m in mine])
        ready[0] += 1
        await started.wait()
        for m in mine:                                          # matches start spread over one ball interval
            m.due = t0 + rng.random() * a.ball_s
        due = time.monotonic()
        while True:
            now = time.monotonic()
            batch = []
            for m in mine:
                while m.due <= now:
                    # A ball arrives at m.due and waits for this write; that wait counts as latency.
                    batch.append(SendMessage(m.ball(rng, int((now - m.due) * 1e9))))
                    m.due += a.ball_s * rng.uniform(0.7, 1.3)
            if batch:
                s0 = time.perf_counter()
                await c.send_messages(STREAM, LANE, pid, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
                sent[0] += len(batch)
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def go():
        nonlocal t0
        while ready[0] < PARTITIONS:
            await asyncio.sleep(0.05)
        print(f"[scoring] line-ups: {len(matches)} live matches across {PARTITIONS} partitions", flush=True)
        gc.freeze()                                             # keep startup objects out of the collector's way
        t0 = time.monotonic()
        started.set()

    async def report():
        await started.wait()
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[scoring] {sent[0]:>5,} balls/s  {r['n']:,} writes  write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms",
                  flush=True)
            sent[0] = 0

    await asyncio.gather(go(), report(), *(writer(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--matches", type=int, default=40, help="live matches at once")
    p.add_argument("--ball-s", type=float, default=2, help="seconds between balls per match (real T20: 30-40)")
    p.add_argument("--tick-ms", type=float, default=2, help="each writer writes once per this many ms")
    asyncio.run(main(p.parse_args()))
