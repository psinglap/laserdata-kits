"""The Play Along scorer: every locked answer scored for its player before the answer is revealed on TV.

Reads two topics directly, each partition on a connection of its own (no consumer group: a group reader
visits one partition per poll, so with 16 partitions a lock waits its turn):
  * show-desk: the question timeline. open (level, points, timer, answer key), close, reveal.
  * answer-locks (the lane): each lock, keyed by player. The scorer
      - drops a repeat (a sequence it has already applied for that player) and a second lock on the same
        question, so every lock counts once;
      - rejects a lock the answer API received after the question closed, even one it scored before it
        read the close (the points are taken back at the close);
      - scores the rest: the question's points for a right answer, plus 200 if the lock came inside the
        first 10 seconds (KBC 2025 Play Along FAQ), into the player's episode total.
    Lock-to-score latency is timed here, and so is the outcome that matters: a valid lock scored only after
    the answer was revealed, when the player has already seen their result and the leaderboard without it.

Every score comes from the log, so a scorer started late, or restarted, rebuilds the same scores: about 5 s
after each reveal it prints a digest of that question's scores per player, to compare across scorers.
--hold-ms is a what-if: score each lock only once it is that old, as a slower path would.
"""
import argparse
import asyncio
import gc
import os
import time
import zlib
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, FAST_BONUS, FAST_S, LOCKS, PARTITIONS, SHOW, STREAM, Window, ensure_topics, load


async def main(a):
    async def own_client():                 # one connection per reader, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LOCKS, SHOW)

    qs = {}                                 # question -> its timeline, answer key and every lock scored on it
    pending = defaultdict(list)             # locks read before their question's open (the show desk is on another partition)
    seqs = {}                               # player -> last applied sequence
    totals = defaultdict(int)               # player -> episode points
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new scorer first replays the episode so far: that is a rebuild, not live
    held = deque()                          # what-if only: locks waiting until they are --hold-ms old
    gc.freeze()

    def score(v, qr):
        p, now = v["player"], time.time_ns()
        if caught_up[0]:
            lat.add((now - v["ns"]) / 1e6)
        else:
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], (now - v["ns"]) / 1e6)
        if p in qr["locks"]:
            stats["second"] += 1            # one lock per question: the first one counts
            return
        if qr["close_ns"] and v["lock_ns"] > qr["close_ns"]:
            qr["late"] += 1                 # reached the answer API after the timer closed
            return
        right = v["opt"] == qr["answer"]
        pts = qr["points"] + (FAST_BONUS if v["lock_ns"] - qr["open_ns"] <= FAST_S * 1e9 else 0) if right else 0
        qr["locks"][p] = (v["lock_ns"], pts, now)
        totals[p] += pts
        qr["right"] += right
        qr["fast"] += right and pts > qr["points"]
        if qr["reveal_ns"] and now > qr["reveal_ns"] and caught_up[0]:
            qr["after"] += 1                # the player saw the reveal before their lock was scored
            stats["after"] += 1
        stats["scored"] += 1

    def apply(v):
        p = v["player"]
        if v["seq"] <= seqs.get(p, 0):
            stats["repeats"] += 1           # older or repeated lock: already applied
            return
        seqs[p] = v["seq"]
        qr = qs.get(v["q"])
        if qr is None:
            pending[v["q"]].append(v)       # its question's open is not read yet
        else:
            score(v, qr)

    async def on_lock(m):
        v = load(m.payload())
        if a.hold_ms:
            held.append(v)
        else:
            apply(v)

    async def release():                    # what-if only: apply held locks once they are old enough
        while True:
            cut = time.time_ns() - a.hold_ms * 1_000_000
            while held and held[0]["ns"] <= cut:
                apply(held.popleft())
            await asyncio.sleep(0.002)

    async def on_show(m):
        v = load(m.payload())
        q = v["q"]
        if v["kind"] == "open":
            qs[q] = qr = {"open_ns": v["ns"], "points": v["points"], "level": v["level"], "answer": v["answer"],
                          "close_ns": 0, "reveal_ns": 0, "locks": {}, "late": 0, "right": 0, "fast": 0, "after": 0,
                          "printed": False}
            for w in pending.pop(q, []):
                score(w, qr)
        elif v["kind"] == "close" and q in qs:
            qr = qs[q]
            qr["close_ns"] = v["ns"]
            for p, (lock_ns, pts, _) in [x for x in qr["locks"].items() if x[1][0] > v["ns"]]:
                del qr["locks"][p]          # scored before the close was read, but received after it: take it back
                totals[p] -= pts
                qr["late"] += 1
                qr["right"] -= pts > 0
                qr["fast"] -= pts > qr["points"]
        elif v["kind"] == "reveal" and q in qs:
            qr = qs[q]
            qr["reveal_ns"] = v["ns"]
            if caught_up[0]:
                qr["after"] = sum(1 for _, _, done in qr["locks"].values() if done > v["ns"])
                stats["after"] += qr["after"]
                n = len(qr["locks"])
                print(f"[reveal] {q} level {qr['level']} ({qr['points']:,} pts)  locks scored {n:,}  "
                      f"right {100 * qr['right'] / max(1, n):.1f}%  10 s bonus {qr['fast']:,}  late rejected {qr['late']:,}  "
                      f"scored after the reveal {qr['after']:,}  top episode score {max(totals.values(), default=0):,}",
                      flush=True)

    async def digests():
        # About 5 s after each reveal, every lock for that question is in: print a digest of its scores per player.
        # The same log gives the same digest on any scorer, live or rebuilt.
        while True:
            await asyncio.sleep(0.5)
            if not caught_up[0]:
                continue
            for q, qr in qs.items():
                if qr["reveal_ns"] and not qr["printed"] and time.time_ns() - qr["reveal_ns"] > a.settle_s * 1e9:
                    qr["printed"] = True
                    rows = "\n".join(f"{p}:{pts}" for p, (_, pts, _) in sorted(qr["locks"].items()))
                    print(f"[digest] {q}  locks {len(qr['locks']):,}  points {sum(x[1] for x in qr['locks'].values()):,}"
                          f"  late {qr['late']:,}  digest {zlib.crc32(rows.encode()):08x}", flush=True)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of locks arrived inside the budget; partitions interleave,
                # so one fresh lock alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms + a.hold_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: rebuilt from {stats['replayed']:,} earlier locks this second, "
                      f"questions {len(qs)}, players {len(totals):,}", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            if r["n"]:
                ok = "PASS" if r["p99"] < a.budget_ms + a.hold_ms else "FAIL"
                print(f"[{a.name}] {r['n']:>7,} locks/s  lock-to-score p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                      f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms + a.hold_ms}ms)  players scored {len(totals):,}",
                      flush=True)
            else:
                print(f"[{a.name}]       0 locks/s  between questions  players scored {len(totals):,}", flush=True)
            print(f"[play-along] scored {stats['scored']:,}  repeats dropped {stats['repeats']:,}  "
                  f"second locks dropped {stats['second']:,}  scored after the reveal {stats['after']:,}", flush=True)
            stats.clear()

    async def read(topic, handler, pids):
        # Each partition in turn, from the start of the log (the scores are rebuilt from it), then from this
        # reader's own position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{topic}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, topic, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        await handler(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    parts = list(range(PARTITIONS[LOCKS]))
    await asyncio.gather(read(SHOW, on_show, [0]), *[read(LOCKS, on_lock, parts[r::a.readers]) for r in range(a.readers)],
                         release(), digests(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="scorer")
    p.add_argument("--budget-ms", type=int, default=100,
                   help="lock-to-score p99 budget (Sony LIV kept the KBC game under 100 ms, per AWS)")
    p.add_argument("--hold-ms", type=int, default=0, help="what-if: score each lock only once it is this old")
    p.add_argument("--readers", type=int, default=2, help="connections reading the lane's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--settle-s", type=float, default=5, help="seconds after a reveal before its digest is printed")
    asyncio.run(main(p.parse_args()))
