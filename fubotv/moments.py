"""Live game moments: every pitch of every live pro baseball game, every key play, its clip and its replay review,
written once to the lane, in order per game.

One writer per partition, each with its own connection, writing once per --tick-ms with what came in since the
last write. Per game, in the order a game produces them:
  * a pitch, with the result when it ends a plate appearance and the resulting score;
  * a key play when the moment model flags one (every home run, every go-ahead or game-tying hit), right after
    the pitch it came from;
  * the clip of that key play, once it is cut from the cloud DVR recording and playable: --clip-s after the play
    (real seconds; cutting a clip is wall-clock work, whatever the game speed);
  * for some home runs, a replay review --review-s after the play: upheld, or overturned (the runs come off the
    score). A review that ends before the clip is ready holds the alert back; one that ends after it is a
    correction.

On start it writes one opening line per game (sequence 1, the score at the start). Game time is compressed: --speed
20 (the default) pitches 20 times faster than real time (a pitch about every second per game instead of every
~20 s) so every second has moments to time; --speed 1 is real time. Teams, plays and clip times are illustrative.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, Window, connect, ensure_topic, game_ids, pack, \
    partition_of, teams_of

PITCH_S = 20.0                                   # real seconds between pitches, between-batter time included
ENDS_PA = 0.27                                   # share of pitches that end a plate appearance
# (result, weight, runs distribution as (runs, weight))
RESULTS = [("out", 66, [(0, 1)]), ("walk", 8, [(0, 95), (1, 5)]), ("single", 15, [(0, 60), (1, 30), (2, 10)]),
           ("double", 5, [(0, 45), (1, 40), (2, 15)]), ("triple", 0.5, [(0, 10), (1, 60), (2, 30)]),
           ("HR", 3.5, [(1, 55), (2, 30), (3, 12), (4, 3)])]
RW = [w for _, w, _ in RESULTS]


class Game:
    def __init__(self, gid: str, teams: tuple, rng, a):
        self.id, self.teams, self.rng, self.a = gid, teams, rng, a
        self.seq = self.play = 0
        self.inn, self.half, self.outs, self.sc = 1, 0, 0, [0, 0]
        self.later = []                          # (due, kind, play, body): clips and reviews still to come
        self.plays = {}                          # play id -> (batting side, runs): for an overturn
        self.due = 0.0

    def frame(self) -> str:
        return f"{'TB'[self.half]}{self.inn}"

    def msg(self, kind, play, body, waited_ns=0) -> bytes:
        self.seq += 1
        return pack(self.id, self.seq, kind, play, {"f": self.frame(), "sc": list(self.sc), **body}, waited_ns)

    def opening(self) -> bytes:
        return self.msg("start", 0, {"r": f"{self.teams[0]} at {self.teams[1]}, first pitch"})

    def pitch(self, now: float, waited_ns: int) -> list:
        rng = self.rng
        if rng.random() >= ENDS_PA:
            return [self.msg("pitch", 0, {"r": rng.choice(["ball", "called strike", "swinging strike", "foul"])},
                             waited_ns)]
        res, _, runs_d = rng.choices(RESULTS, RW)[0]
        runs = rng.choices([r for r, _ in runs_d], [w for _, w in runs_d])[0]
        bat = self.half                          # 0: away bats in the top, 1: home in the bottom
        before = self.sc[bat] - self.sc[1 - bat]
        self.sc[bat] += runs
        after = self.sc[bat] - self.sc[1 - bat]
        out = [self.msg("pitch", 0, {"r": res, "runs": runs}, waited_ns)]
        label = None
        if before <= 0 < after:
            label = f"go-ahead {res}"
        elif before < 0 == after:
            label = f"game-tying {res}"
        elif res == "HR":
            label = "HR" if runs < 4 else "grand slam"
        if label:                                # the moment model flags it: one key play, then its clip
            self.play += 1
            self.plays[self.play] = (bat, runs)
            out.append(self.msg("key", self.play, {"t": self.teams[bat], "e": label, "runs": runs}, waited_ns))
            self.later.append((now + rng.uniform(*self.a.clip_s), "clip", self.play,
                               {"c": f"{self.id}-{self.play}", "len": rng.randint(12, 40)}))
            if res == "HR" and rng.random() < self.a.review_share:
                self.later.append((now + rng.uniform(*self.a.review_s), "review", self.play,
                                   {"v": "overturned" if rng.random() < 0.5 else "upheld"}))
        if res == "out":
            self.outs += 1
            if self.outs == 3:
                self.outs, self.half = 0, 1 - self.half
                if self.half == 0:
                    self.inn += 1
                    if self.inn > 9:             # final: the next game on the same id starts from 0-0
                        self.inn, self.sc = 1, [0, 0]
        return out

    def take(self, now: float, speed: float) -> list:
        out = []
        while True:
            nxt = min(self.later, key=lambda x: x[0]) if self.later else None
            t = min(self.due, nxt[0]) if nxt else self.due
            if t > now:
                return out
            waited = int((now - t) * 1e9)       # the entry waited this long for the write: it counts as latency
            if nxt and nxt[0] <= self.due:
                self.later.remove(nxt)
                _, kind, play, body = nxt
                if kind == "review" and body["v"] == "overturned" and play in self.plays:
                    bat, runs = self.plays[play]
                    self.sc[bat] = max(0, self.sc[bat] - runs)   # the runs come off the score
                out.append(self.msg(kind, play, body, waited))
            else:
                out += self.pitch(self.due, waited)
                self.due += PITCH_S / speed * self.rng.uniform(0.6, 1.4)
            if len(self.plays) > 200:
                for k in sorted(self.plays)[:50]:
                    del self.plays[k]


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, LANE, PARTITIONS)
    rng = random.Random(7)
    games = [Game(g, teams_of(i), rng, a) for i, g in enumerate(game_ids(a.games))]
    by_part = [[g for g in games if partition_of(g.id) == p] for p in range(PARTITIONS)]
    acks, sent, keys, clips, t0 = Window(), [0], [0], [0], time.monotonic()
    ready, started = [0], asyncio.Event()

    async def writer(pid):
        c = await connect(a.url)
        mine = by_part[pid]
        if mine:
            await c.send_messages(STREAM, LANE, pid, [SendMessage(g.opening()) for g in mine])
        ready[0] += 1
        await started.wait()
        for g in mine:                           # games spread their first pitch over one pitch gap
            g.due = t0 + rng.random() * PITCH_S / a.speed
        due = time.monotonic()
        while True:
            now = time.monotonic()
            batch = []
            for g in mine:
                for b in g.take(now, a.speed):
                    batch.append(SendMessage(b))
                    keys[0] += b'"k":"key"' in b
                    clips[0] += b'"k":"clip"' in b
            if batch:
                s0 = time.perf_counter()
                await c.send_messages(STREAM, LANE, pid, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
                sent[0] += len(batch)
            due += a.tick_ms / 1000              # from the due time, so a slow tick doesn't drift
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def go():
        nonlocal t0
        while ready[0] < PARTITIONS:
            await asyncio.sleep(0.05)
        print(f"[moments] opening lines: {len(games)} live games across {PARTITIONS} partitions, pitches at "
              f"{a.speed:g}x, clips ready {a.clip_s[0]:g}-{a.clip_s[1]:g} s and reviews "
              f"{a.review_s[0]:g}-{a.review_s[1]:g} s after the play", flush=True)
        gc.freeze()                              # keep startup objects out of the collector's way
        t0 = time.monotonic()
        started.set()

    async def report():
        await started.wait()
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[moments] {sent[0]:>5,} moments/s  key plays {keys[0]}  clips ready {clips[0]}  {r['n']:,} writes  "
                  f"write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms", flush=True)
            sent[0] = keys[0] = clips[0] = 0

    await asyncio.gather(go(), report(), *(writer(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--games", type=int, default=15, help="live games at once (a full slate is 15)")
    p.add_argument("--speed", type=float, default=20, help="game time vs real time (1 = real time)")
    p.add_argument("--clip-s", type=lambda s: tuple(float(x) for x in s.split(",")), default=(4.0, 12.0),
                   help="seconds from a key play to its clip being playable, as min,max (illustrative)")
    p.add_argument("--review-s", type=lambda s: tuple(float(x) for x in s.split(",")), default=(30.0, 120.0),
                   help="seconds from a reviewed home run to the review's result, as min,max (illustrative)")
    p.add_argument("--review-share", type=float, default=0.15, help="share of home runs that go to replay review")
    p.add_argument("--tick-ms", type=float, default=2, help="each writer writes once per this many ms")
    asyncio.run(main(p.parse_args()))
