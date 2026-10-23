"""The review service: writes pull-request review thread changes into Apache Iggy.

At start it opens --threads review threads (the snapshot, seq 1, marked "snap" and not timed). Then, at --rate
changes a second, a reviewer comments on or edits an open thread (the message carries the thread's whole current
state and the next seq), or a thread is resolved (a {"_deleted": id} message) and a new one opens in its place, so
the number of open threads stays the same. Sends one batch per 10 ms tick. --spike-x multiplies the rate for
--spike-for seconds from --spike-at (a bot commenting on every open pull request after a dependency bump).
"""
import argparse
import asyncio
import gc
import json
import random
import string
import time

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import COMMENTS, DEFAULT_URL, EVENTS, PATHS, REPOS, STREAM, TOPICS, Window, ensure_topic

AUTHORS = ["reviewer-%02d" % i for i in range(60)] + ["ci-bot", "security-bot"]


def dumps(d):
    return json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topic(client, a.topic)
    run = "".join(random.choices(string.ascii_lowercase, k=4))   # thread ids never collide across runs
    state, open_ids, counter = {}, [], 0

    def new_thread(ns, snap):
        nonlocal counter
        counter += 1
        repo = random.choice(REPOS)
        t = {"thread_id": f"{run}-{counter:06d}", "seq": 1, "ns": ns, "repo": repo, "pr": random.randint(1000, 9999),
             "path": random.choice(PATHS).format(random.choice(TOPICS)), "line": random.randint(1, 800),
             "state": "open", "author": random.choice(AUTHORS), "comments": 1, "body": random.choice(COMMENTS)}
        state[t["thread_id"]] = t
        open_ids.append(t["thread_id"])
        return dumps({**t, "snap": True} if snap else t)

    ns = time.time_ns()
    snap = [SendMessage(new_thread(ns, True)) for _ in range(a.threads)]
    for i in range(0, len(snap), 1000):
        await client.send_messages(STREAM, a.topic, Partitioning.partition_id(0), snap[i:i + 1000])
    print(f"[review] opened {a.threads:,} review threads across {len(REPOS)} repositories (snapshot, seq 1)", flush=True)
    gc.freeze()                                    # no collector pauses in the hot path once started

    sent, resolved, total, send_ms = 0, 0, 0, Window()
    tick, start = 0.01, time.monotonic()
    next_t, carry, last_report = start, 0.0, start
    while True:
        now = time.monotonic()
        t = now - start
        if t >= a.duration:
            break
        rate = a.rate * (a.spike_x if a.spike_at <= t < a.spike_at + a.spike_for else 1)
        carry += rate * tick
        n, carry = int(carry), carry - int(carry)
        batch, ns = [], time.time_ns()
        for _ in range(n):
            if random.random() < a.resolve:
                i = random.randrange(len(open_ids))
                tid = open_ids[i]
                open_ids[i] = open_ids[-1]
                open_ids.pop()
                th = state.pop(tid)
                batch.append(SendMessage(dumps({"_deleted": tid, "seq": th["seq"] + 1, "ns": ns})))
                batch.append(SendMessage(new_thread(ns, False)))
                resolved += 1
            else:
                th = state[random.choice(open_ids)]
                th["seq"] += 1
                th["ns"] = ns
                th["comments"] += 1
                th["author"] = random.choice(AUTHORS)
                th["body"] = random.choice(COMMENTS)
                batch.append(SendMessage(dumps(th)))
        if batch:
            t0 = time.perf_counter()
            await client.send_messages(STREAM, a.topic, Partitioning.partition_id(0), batch)
            send_ms.add((time.perf_counter() - t0) * 1000)
            sent += len(batch)
            total += len(batch)
        if now - last_report >= 1:
            last_report += 1
            w = send_ms.flush()
            print(f"[review] {sent:>6,} changes/s ({resolved} resolved)  open threads {len(open_ids):,}  "
                  f"send p50 {w['p50']:4.1f}ms p99 {w['p99']:4.1f}ms  total {total:,}", flush=True)
            sent = resolved = 0
        next_t += tick                             # schedule from the due time, so a slow tick doesn't drift
        await asyncio.sleep(max(0.0, next_t - time.monotonic()))
    print(f"[review] done: {total:,} changes after the snapshot; {len(open_ids):,} threads open", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--topic", default=EVENTS)
    p.add_argument("--threads", type=int, default=20000, help="open review threads (kept constant)")
    p.add_argument("--rate", type=float, default=300, help="thread changes per second")
    p.add_argument("--resolve", type=float, default=0.08, help="share of changes that resolve a thread")
    p.add_argument("--duration", type=float, default=3600)
    p.add_argument("--spike-x", type=float, default=1, help="rate multiplier during the spike")
    p.add_argument("--spike-at", type=float, default=1e9)
    p.add_argument("--spike-for", type=float, default=0)
    asyncio.run(main(p.parse_args()))
