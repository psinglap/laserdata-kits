"""The conversational layer's voice hosts, fed by the task progress lane.

Plays --hosts voice hosts. A call's audio session lives on one host, and that host reads only the task-events
partitions its calls hash to, directly, partition by partition, on its own connection (no consumer group: a
group reader visits one partition per poll, so a step would wait for its partition's turn). Each record is one
step of one task behind one call, with the call's sequence, so a replayed or repeated step changes nothing.

While a call has a task running, the conversational layer speaks about it every --speak-gap seconds: a progress
update ("still checking later flights"), or an answer to the caller's follow-up ("is it done yet?"). Whatever it
says comes from the newest step this host has. Once the result is in, the next update speaks the result and the
call goes back to conversation.

The business number: spoken updates that were out of date, because the reasoning layer had already published a
newer step of the same task when the update was spoken, and the step had not reached this host yet. The worst
case is called out: the agent said it was still working after the result was in. Prints it every second, with
what a --compare-ms lane would have said out of date at the same moments, and event-to-voice p50/p99/p99.9 with
PASS/FAIL against --budget-ms.

A restarted host replays its partitions from the start, rebuilds every live call's task, and starts speaking
again only for steps newer than --replay-s.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, DONE, STREAM, TASKS, Window, ensure_topics, host_of, load


async def main(a):
    async def own_client():                 # one connection per host
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), TASKS)
    parts = list(range((await (await own_client()).get_topic(STREAM, TASKS)).partitions_count))
    state = {}                              # call -> [seq, status, step, of, say, step ns, task]
    spoken = {}                             # call -> [spoken ns, seq spoken, out of date, out of date at compare, task]
    ticks, next_tick = [], {}               # (monotonic due, call); call -> its current due time
    lat, stats = Window(), defaultdict(float)
    caught_up = [False]                     # a new host first replays the steps from before it joined
    cmp_ns = int(a.compare_ms * 1e6)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_step(m):
        d = load(m.payload())
        now = time.time_ns()
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest step seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        c = d["call"]
        st = state.get(c)
        if st and d["seq"] <= st[0]:
            stats["dropped"] += 1           # older or repeated step: already applied
            return
        state[c] = [d["seq"], d["status"], d["step"], d["of"], d["say"], d["ns"], d["task"]]
        stats["events"] += 1
        sp = spoken.get(c)
        # The last update spoken on this call came after this step happened, about the same task, from an older
        # step: it was out of date when it was said.
        if sp and sp[4] == d["task"] and sp[0] > d["ns"] and sp[1] < d["seq"] and not sp[2]:
            sp[2] = True
            stats["stale"] += 1
            stats["stale_done"] += d["status"] == DONE
            if not sp[3]:
                sp[3] = True
                stats["cmp"] += 1
        if c not in next_tick and age < a.replay_s * 1000:
            t = time.monotonic() + random.uniform(*a.speak_gap)
            next_tick[c] = t
            heapq.heappush(ticks, (t, c))

    async def speak():
        # The conversational layer's cadence while tasks run; it never waits on the lane, it says what it has.
        while True:
            await asyncio.sleep(0.01)
            now = time.monotonic()
            while ticks and ticks[0][0] <= now:
                t, c = heapq.heappop(ticks)
                if next_tick.get(c) != t:
                    continue
                seq, status, step, of, say, step_ns, task = state[c]
                t_ns = time.time_ns()
                # At --compare-ms a step this new would not have arrived yet (the task's first step only starts
                # the cadence, so it is not counted).
                late = step > 1 and t_ns - step_ns < cmp_ns
                spoken[c] = [t_ns, seq, False, late, task]
                stats["spoken"] += 1
                stats["cmp"] += late
                if status == DONE:
                    stats["results"] += 1
                    del next_tick[c]        # result spoken: back to conversation
                else:
                    nt = now + random.uniform(*a.speak_gap)
                    next_tick[c] = nt
                    heapq.heappush(ticks, (nt, c))

    async def read(h, pids):
        # Each partition in turn, from the start of the log (live calls' tasks are rebuilt from it), then from
        # our own position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"voice-host-{h:02d}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, TASKS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_step(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        print(f"[voice] {a.hosts} voice hosts reading {len(parts)} task-events partitions, an update every "
              f"{a.speak_gap[0]:g}-{a.speak_gap[1]:g} s while a task runs", flush=True)
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of steps arrived inside the budget; partitions interleave, so one
                # fresh step alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[voice] catching up: replayed {int(stats['replayed']):,} earlier steps this second, "
                      f"task state rebuilt for {len(state):,} calls", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[voice] {r['n']:>7,} task events/s  event-to-voice p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  updates spoken {int(stats['spoken']):,}, "
                  f"out of date {int(stats['stale']):,} (still working after the result {int(stats['stale_done']):,}) "
                  f"(at {a.compare_ms:g} ms it would be {int(stats['cmp']):,})  results spoken {int(stats['results']):,}  "
                  f"calls on a task {len(next_tick):,}", flush=True)
            stats.clear()

    await asyncio.gather(speak(), report(),
                         *(read(h, [p for p in parts if host_of(p, a.hosts) == h]) for h in range(a.hosts)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--hosts", type=int, default=8, help="voice hosts, each reading its own calls' partitions")
    p.add_argument("--speak-gap", type=float, nargs=2, default=(1.0, 3.0), metavar=("MIN", "MAX"),
                   help="seconds between spoken updates while a task runs (illustrative)")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--budget-ms", type=int, default=50, help="step-to-voice-host p99 budget for the lane")
    p.add_argument("--compare-ms", type=float, default=1000, help="also count updates a lane this slow would get wrong")
    p.add_argument("--replay-s", type=float, default=10, help="after a restart, speak again only for steps younger than this")
    asyncio.run(main(p.parse_args()))
