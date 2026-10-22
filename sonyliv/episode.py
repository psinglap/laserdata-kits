"""An episode of KBC with Play Along: the show desk asks questions, and players lock answers in the Sony LIV app.

Plays two sides of a live episode:
  * the show desk, on show-desk (one partition, in order): each question opens with its level, points and
    timer; it closes when the timer runs out, or earlier when the next question is asked on TV (the timer does
    not extend past that, per the Play Along FAQ); then the answer is revealed on TV and in the app at once.
    The open message also carries the answer key, for the scorer only.
  * the answer API, on answer-locks (the lane): every answer a player locks, keyed by player, stamped with
    the time the API received it (lock_ns) and a sequence that only grows for that player. Most players lock
    inside the first 10 seconds, when a correct answer earns 200 bonus points, so each question starts with
    a burst. A few locks reach the API after the timer has closed (a call, a weak network), and a few are
    sent twice (a retry). The scorer must reject the first and count the second once.

--timer-s, --reveal-gap-s and --next-gap-s shape the question timeline; --players and --answer-share the size.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (DEFAULT_URL, FAST_S, LOCKS, PARTITIONS, POINTS, SHOW, STREAM, ensure_topics, msg, part,
                    player_id, question_id)

TIMERS = [30] * 5 + [45] * 5 + [60] * 6        # illustrative timer per level; Sony LIV sets it per question


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LOCKS, SHOW)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    lock_p = await (await own_client()).producer(STREAM, LOCKS, topic_partitions_count=PARTITIONS[LOCKS], mode=cfg)
    show_p = await (await own_client()).producer(STREAM, SHOW, topic_partitions_count=PARTITIONS[SHOW], mode=cfg)
    # Lock sequence: one counter for all players, started from the clock, so each player's sequence only grows,
    # even across restarts of this script, without keeping a counter per player.
    next_seq = iter(range(time.time_ns(), 1 << 63))
    stats = defaultdict(int)
    state = {"q": "", "phase": "between questions", "t": 0.0, "timer": 0}
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def send_locks(items):
        # One batch per partition, all sent at once: per-player order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for player, m in items:
            groups[part(player, PARTITIONS[LOCKS])].append(m)
        await asyncio.gather(*(lock_p.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    def lock(player, q, opt):
        s, now = next(next_seq), time.time_ns()
        m = SendMessage(msg(player=player, q=q, opt=opt, seq=s, lock_ns=now, ns=now))
        out = [(player, m)]
        if random.random() < a.repeat_share:            # the app retried: same lock, same sequence
            out.append((player, SendMessage(msg(player=player, q=q, opt=opt, seq=s, lock_ns=now, ns=now))))
            stats["repeats"] += 1
        return out

    async def question(n):
        level = (n - 1) % len(POINTS)
        q = question_id(a.episode, n)
        timer = a.timer_s or TIMERS[level]
        answer = random.choice("ABCD")
        # Harder questions: fewer right answers (illustrative).
        right = max(0.2, 0.9 - 0.045 * level)
        close_at = timer * random.uniform(0.6, 0.9) if random.random() < a.cut_share else timer
        who = random.sample(range(a.players), int(a.players * a.answer_share))
        plan = []                                       # (seconds after open, player, option)
        for i in who:
            t = random.uniform(a.react_s, FAST_S) if random.random() < a.fast_share else random.uniform(FAST_S, timer)
            opt = answer if random.random() < right else random.choice([o for o in "ABCD" if o != answer])
            plan.append((t, player_id(i), opt))
        plan.sort(reverse=True)                         # pop from the end: earliest first
        t0 = time.monotonic()
        await show_p.send([SendMessage(msg(kind="open", q=q, level=level + 1, points=POINTS[level], timer_s=timer,
                                           answer=answer))])
        state.update(q=q, phase="open", t=t0, timer=timer)
        due = t0
        while True:                                     # until the timer runs out or the next question is asked
            el = time.monotonic() - t0
            if el >= close_at:
                break
            items = []
            while plan and plan[-1][0] <= el:
                _, p, opt = plan.pop()
                items += lock(p, q, opt)
            stats["locks"] += len(items)
            if items:
                await send_locks(items)
            due += 0.01                                 # from the due time, so a slow tick doesn't drift the schedule
            await asyncio.sleep(max(0.0, due - time.monotonic()))
        await show_p.send([SendMessage(msg(kind="close", q=q, cut=close_at < timer))])
        state["phase"] = "closed"
        stats["cut"] += close_at < timer
        # Players who had not locked by the close see the timer end in the app. A few locks were already on the
        # way and reach the answer API after the close: the scorer must reject them.
        late = sorted(((random.uniform(0.05, a.late_window_s), p, o) for _, p, o in plan[:int(len(who) * a.late_share)]),
                      reverse=True)
        t1 = due = time.monotonic()
        while late:
            el = time.monotonic() - t1
            items = []
            while late and late[-1][0] <= el:
                _, p, opt = late.pop()
                items += lock(p, q, opt)
            stats["late"] += len(items)
            stats["locks"] += len(items)
            if items:
                await send_locks(items)
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
        await asyncio.sleep(max(0.0, t1 + a.reveal_gap_s - time.monotonic()))
        await show_p.send([SendMessage(msg(kind="reveal", q=q, answer=answer))])
        state["phase"] = "revealed"
        print(f"[episode] {q} revealed: answer {answer}, {len(who):,} players locked or tried, "
              f"closed at {close_at:.1f} s of {timer} s{' (next question asked)' if close_at < timer else ''}",
              flush=True)

    async def run():
        async with lock_p, show_p:
            n = 1
            while True:
                await question(n)
                state["phase"] = "between questions"
                await asyncio.sleep(a.next_gap_s)
                n += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            ph = state["phase"]
            if ph == "open":
                ph = f"{state['q']} open {time.monotonic() - state['t']:4.1f} s of {state['timer']} s"
            print(f"[episode] {stats['locks']:>7,} locks/s  {ph}  late {stats['late']:,}  repeats {stats['repeats']:,}",
                  flush=True)
            for k in ("locks", "late", "repeats"):
                stats[k] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--episode", type=int, default=61)
    p.add_argument("--players", type=int, default=100_000,
                   help="players on Play Along (Sony LIV reported a 1.6 million simultaneous peak; a laptop runs a slice)")
    p.add_argument("--answer-share", type=float, default=0.8, help="share of players who lock an answer per question")
    p.add_argument("--fast-share", type=float, default=0.7, help="share of locks inside the first 10 s (the bonus)")
    p.add_argument("--react-s", type=float, default=1.5, help="seconds before the first player locks")
    p.add_argument("--timer-s", type=float, default=0, help="timer for every question; 0 uses an illustrative per-level timer")
    p.add_argument("--cut-share", type=float, default=0.15, help="questions closed early because the next one was asked")
    p.add_argument("--late-share", type=float, default=0.01, help="share of players whose lock arrives after the close")
    p.add_argument("--late-window-s", type=float, default=1.5, help="late locks arrive up to this long after the close")
    p.add_argument("--repeat-share", type=float, default=0.01, help="share of locks the app sends twice")
    p.add_argument("--reveal-gap-s", type=float, default=4, help="seconds from the close to the reveal on TV")
    p.add_argument("--next-gap-s", type=float, default=6, help="seconds from the reveal to the next question")
    asyncio.run(main(p.parse_args()))
