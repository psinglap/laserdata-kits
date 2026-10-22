"""The commentary desk: every ball-by-ball line of every live match, and every correction, written once to the lane.

One writer per partition, each with its own connection, writing once per --tick-ms with the lines that came in
since the last write, so every match's lines stay in order. A message is one entry (see common.py): a new line
for a delivery, an end-of-over summary, or an edit to a line already published, each with the resulting score.
Corrections happen the way a desk makes them: a wicket overturned on review (the line becomes NOT OUT and a
wicket comes off the score, before the next ball) and a boundary re-scored as leg byes a ball or two later.

On start it writes one opening line per match (sequence 1, the score at the start). Match time is compressed:
--speed 20 (the default) bowls 20 times faster than real time (a T20 ball every ~2 s instead of ~40 s) so every
second has lines to time; --speed 1 is real time. Matches, players and text are illustrative.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, FORMATS, LANE, PARTITIONS, STREAM, Window, connect, ensure_topic, format_of, \
    match_ids, pack, partition_of

# (label, runs, legal, weight, words)
BALLS = [("no run", 0, True, 36, "defended back to the bowler"), ("1 run", 1, True, 32, "worked into the leg side"),
         ("2 runs", 2, True, 8, "pushed into the gap, they come back for two"),
         ("FOUR", 4, True, 11, "driven through the covers, races away"),
         ("SIX", 6, True, 5, "launched over long-on"), ("OUT", 0, True, 5, "edged and taken behind"),
         ("wide", 1, False, 3, "down the leg side, called wide")]
W = [w for *_, w, _ in BALLS]


class Match:
    def __init__(self, mid: str, fmt: str, rng):
        self.id, self.fmt, self.rng = mid, fmt, rng
        self.seq = self.line = 0
        self.gap, self.limit = FORMATS[fmt]
        self.inn, self.sc = 0, [0, 0, 0]                 # innings; runs, wickets, legal balls of the batting side
        self.lines = {}                                  # line id -> (version, over, event, text): for corrections
        self.edits = []                                  # (due, kind, line) corrections the desk will make
        self.due = 0.0

    def score(self) -> list:
        return [self.inn, *self.sc]

    def msg(self, kind, line, ver, over, event, text, waited_ns=0) -> bytes:
        self.seq += 1
        return pack(self.id, self.seq, kind, line, ver, over, event, text, self.score(), waited_ns)

    def opening(self) -> bytes:
        return self.msg("start", 0, 1, "0.0", "START", f"{self.fmt} match {self.id}, first innings under way")

    def ball(self, now: float, speed: float, waited_ns: int) -> list:
        rng, sc = self.rng, self.sc
        label, runs, legal, _, words = rng.choices(BALLS, W)[0]
        sc[0] += runs
        if legal:
            sc[2] += 1
        if label == "OUT":
            sc[1] += 1
        o = f"{(sc[2] - 1) // 6}.{(sc[2] - 1) % 6 + 1}" if legal else f"{sc[2] // 6}.{sc[2] % 6 + 1}"
        self.line += 1
        text = f"bowler {rng.randint(1, 6)} to batter {rng.randint(1, 11)}, {label}, {words}"
        self.lines[self.line] = (1, o, label, text)
        out = [self.msg("ball", self.line, 1, o, label, text, waited_ns)]
        ended = sc[1] >= 10 or (self.limit is not None and sc[2] >= self.limit)
        gap = self.gap / speed
        if label == "OUT" and not ended and rng.random() < 0.2:
            self.edits.append((now + gap * 0.5, "overturn", self.line))     # reviewed: overturned before the next ball
        elif label == "FOUR" and rng.random() < 0.08:
            self.edits.append((now + gap * 1.2, "legbyes", self.line))      # re-scored a ball later
        if legal and sc[2] % 6 == 0 and not ended:
            out.append(self.msg("over", self.line, 1, f"{sc[2] // 6}.0", "OVER",
                                f"end of over {sc[2] // 6}: {sc[0]}/{sc[1]}", waited_ns))
        if ended:
            self.inn = (self.inn + 1) % 2                # second innings, or a new match on the same id
            self.sc = [0, 0, 0]
            self.edits = []                              # no corrections across an innings change in this kit
        return out

    def edit(self, kind: str, line: int, waited_ns: int) -> bytes:
        ver, o, label, text = self.lines[line]
        if kind == "overturn":
            self.sc[1] -= 1                              # the wicket comes off the score
            label, text = "NOT OUT", text.replace("OUT, edged and taken behind",
                                                  "NOT OUT, overturned on review, missed the edge")
        else:
            label, text = "4 leg byes", text.replace("FOUR, driven through the covers, races away",
                                                       "4 leg byes, off the pad, races away to fine leg")
        self.lines[line] = (ver + 1, o, label, text)
        return self.msg("edit", line, ver + 1, o, label, text, waited_ns)

    def take(self, now: float, speed: float) -> list:
        out = []
        while True:
            e = min(self.edits) if self.edits else None
            t = min(self.due, e[0]) if e else self.due
            if t > now:
                return out
            waited = int((now - t) * 1e9)               # the line waited this long for the write: it counts as latency
            if e and e[0] <= self.due:
                self.edits.remove(e)
                out.append(self.edit(e[1], e[2], waited))
            else:
                out += self.ball(self.due, speed, waited)
                self.due += self.gap / speed * self.rng.uniform(0.6, 1.4)
            if len(self.lines) > 400:                    # keep recent lines only: corrections come within a ball or two
                for k in sorted(self.lines)[:100]:
                    del self.lines[k]


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, LANE, PARTITIONS)
    rng = random.Random(11)
    matches = [Match(m, format_of(i), rng) for i, m in enumerate(match_ids(a.matches))]
    by_part = [[m for m in matches if partition_of(m.id) == p] for p in range(PARTITIONS)]
    acks, sent, edits, t0 = Window(), [0], [0], time.monotonic()
    ready, started = [0], asyncio.Event()

    async def writer(pid):
        c = await connect(a.url)
        mine = by_part[pid]
        if mine:
            await c.send_messages(STREAM, LANE, pid, [SendMessage(m.opening()) for m in mine])
        ready[0] += 1
        await started.wait()
        for m in mine:                                   # matches spread their first ball over one delivery gap
            m.due = t0 + rng.random() * m.gap / a.speed
        due = time.monotonic()
        while True:
            now = time.monotonic()
            batch = []
            for m in mine:
                for b in m.take(now, a.speed):
                    batch.append(SendMessage(b))
                    if b"\"k\":\"edit\"" in b:
                        edits[0] += 1
            if batch:
                s0 = time.perf_counter()
                await c.send_messages(STREAM, LANE, pid, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
                sent[0] += len(batch)
            due += a.tick_ms / 1000                      # from the due time, so a slow tick doesn't drift
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def go():
        nonlocal t0
        while ready[0] < PARTITIONS:
            await asyncio.sleep(0.05)
        fmts = {}
        for m in matches:
            fmts[m.fmt] = fmts.get(m.fmt, 0) + 1
        print(f"[desk] opening lines: {len(matches)} live matches ("
              + ", ".join(f"{n} {f}" for f, n in fmts.items()) + f") across {PARTITIONS} partitions", flush=True)
        gc.freeze()                                      # keep startup objects out of the collector's way
        t0 = time.monotonic()
        started.set()

    async def report():
        await started.wait()
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[desk] {sent[0]:>5,} lines/s  corrections {edits[0]}  {r['n']:,} writes  write ack p50 "
                  f"{r['p50']:.1f}ms  p99 {r['p99']:.1f}ms", flush=True)
            sent[0] = edits[0] = 0

    await asyncio.gather(go(), report(), *(writer(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--matches", type=int, default=30, help="live matches at once, all formats")
    p.add_argument("--speed", type=float, default=20, help="match time vs real time (1 = real time)")
    p.add_argument("--tick-ms", type=float, default=2, help="each writer writes once per this many ms")
    asyncio.run(main(p.parse_args()))
