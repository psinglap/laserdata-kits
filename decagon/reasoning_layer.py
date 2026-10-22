"""The reasoning layer behind every live call: each task's steps onto the task progress lane as they happen.

Plays the more powerful model and its tool calls for --calls live voice calls. Each call alternates between
conversation (illustrative gaps of 4-12 s, see common.IDLE_S) and a task from common.TASK_STEPS (rebook a flight,
check a refund, replace a card, ...), whose steps each take one tool call (0.4-2.5 s, common.STEP_MS). Every
step is published to task-events the moment it happens, keyed by call:
  * call, task, step number and step count, running or done, what the caller can be told, the call's sequence,
    and the time the step happened (the kit's ground truth for "the result was in").

Each record carries the task's resulting state, never "next step", so a voice host that misses or repeats a
record converges on the next one, and a restarted host rebuilds every live call's task from the log.

One batch per partition per 10 ms tick, sent together, so a call's steps stay in order with one round trip per
tick. Prints events, tasks started and results ready every second.
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (DEFAULT_URL, DONE, IDLE_S, PARTITIONS, RUNNING, STEP_MS, STREAM, TASK_NAMES, TASK_STEPS,
                    TASKS, call_id, ensure_topics, idle_gap, msg, part, step_gap)


async def main(a):
    client = IggyClient.from_connection_string(a.url)     # one connection for this producer
    await client.connect()
    await ensure_topics(client, TASKS)
    producer = await client.producer(STREAM, TASKS, topic_partitions_count=PARTITIONS[TASKS],
                                     mode=DirectProducerConfig(batch_length=1000,
                                                               linger_time=timedelta(microseconds=200)))
    calls = [call_id(i) for i in range(a.calls)]
    pid = [part(c, PARTITIONS[TASKS]) for c in calls]
    seq = [0] * a.calls
    task = [None] * a.calls                 # (task id, name, step index) while a task runs
    n_task = [0] * a.calls
    now = time.monotonic()
    due = [(now + random.uniform(0, IDLE_S[1]), i) for i in range(a.calls)]   # calls start mid-conversation
    heapq.heapify(due)
    stats = defaultdict(int)
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def run():
        t0 = tick = time.monotonic()
        async with producer:
            while True:
                now = time.monotonic()
                x = a.peak if a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1
                batches = defaultdict(list)
                while due and due[0][0] <= now:
                    _, i = heapq.heappop(due)
                    if task[i] is None:     # conversation over, a task starts: its first step
                        name = random.choice(TASK_NAMES)
                        n_task[i] += 1
                        task[i] = (f"{name}#{n_task[i]}", name, 0)
                        stats["started"] += 1
                    else:                   # the step's tool call returned: the next step, or the result
                        tid, name, s = task[i]
                        task[i] = (tid, name, s + 1)
                    tid, name, s = task[i]
                    steps = TASK_STEPS[name]
                    status = DONE if s == len(steps) - 1 else RUNNING
                    seq[i] += 1
                    batches[pid[i]].append(SendMessage(msg(
                        call=calls[i], task=tid, step=s + 1, of=len(steps), status=status, say=steps[s], seq=seq[i])))
                    stats["events"] += 1
                    if status == DONE:
                        stats["done"] += 1
                        task[i] = None
                        heapq.heappush(due, (now + idle_gap() / x, i))
                    else:
                        heapq.heappush(due, (now + step_gap(), i))
                # One batch per partition, all sent at once: per-call order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["peak"] = x > 1
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[reasoning] {a.calls:,} live calls, a task after {IDLE_S[0]:g}-{IDLE_S[1]:g} s of conversation, "
              f"steps of {STEP_MS[0] / 1000:g}-{STEP_MS[1] / 1000:g} s", flush=True)
        while True:
            await asyncio.sleep(1)
            phase = "surge" if stats["peak"] else "steady"
            running = sum(1 for t in task if t is not None)
            print(f"[reasoning] {stats['events']:>7,} task events/s ({phase})  tasks started {stats['started']:,}  "
                  f"results ready {stats['done']:,}  tasks running {running:,}", flush=True)
            for k in ("events", "started", "done"):
                stats[k] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--calls", type=int, default=20_000, help="live voice calls")
    p.add_argument("--peak", type=float, default=1, help="task-rate multiplier during a surge (an outage, a storm)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
