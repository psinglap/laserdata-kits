"""OpsAI's side of the command lane: investigations that send kubectl commands to customer clusters and wait
for the replies.

Keeps --investigations open at once, each on a random cluster out of --clusters, the way OpsAI picks up an
alert and works it: four triage reads at once (pods, events, top, the deployment), the logs, then in Auto Fix
mode (--autofix share of investigations) one write (rollout restart, scale, or new limits) and a rollout
status to verify. Between steps it thinks for --think seconds (the model choosing the next command).

Every request goes on kubectl-requests, keyed by cluster, with a request ID that is unique by construction
(run ID + counter), the command, read or write, and the deadline: sent time + timeout_seconds. OpsAI waits
for the reply until that deadline and then gives up: the investigation goes to on-call. Replies come back on
kubectl-replies, read directly from every partition (no consumer group), and are matched by request ID; a
second reply for the same ID is counted and dropped.

Prints a line a second: the lane's latency (request to agent, reply to OpsAI, and the two together, with
PASS/FAIL against --budget-ms), then the command funnel: sent, answered inside the deadline, answered
after OpsAI gave up, expired and not run, stopped at the deadline, duplicates dropped, and investigations
finished or handed to on-call.
"""
import argparse
import asyncio
import gc
import os
import random
import secrets
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (APPS, DEFAULT_URL, FIXES, LOGS_CMD, PARTITIONS, REPLIES, REQUESTS, SLOW_LOGS, STREAM, TRIAGE,
                    VERIFY, Window, clamp_timeout, cluster, ensure_topics, load, msg, part)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), REQUESTS, REPLIES)
    # A short linger: the per-partition sends of one tick go out together, not paced apart.
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, REQUESTS, topic_partitions_count=PARTITIONS[REQUESTS],
                                                   mode=cfg)
    run = secrets.token_hex(3)              # request IDs: run + counter, so two requests never share an ID
    counter = [0]
    start_ns = time.time_ns()
    waiting = {}                            # request ID -> (future, deadline ns)
    gave_up = set()                         # request IDs OpsAI stopped waiting for
    seen = set()                            # request IDs with a reply already
    sent_at = {}                            # request ID -> sent ns, to find requests never answered
    pending = defaultdict(list)             # request partition -> requests waiting for the next send
    q_lat, r_lat, rt_lat = Window(), Window(), Window()
    stats, total = defaultdict(int), defaultdict(int)
    storm = {"on": False}

    def send(c, cmd, kind, sim, timeout_s):
        counter[0] += 1
        rid = f"{run}-{counter[0]}"
        now = time.time_ns()
        dl = now + int(clamp_timeout(timeout_s) * 1e9)
        fut = asyncio.get_running_loop().create_future()
        waiting[rid] = (fut, dl)
        sent_at[rid] = dl
        pending[part(c, PARTITIONS[REQUESTS])].append(
            SendMessage(msg(ns=now, id=rid, c=c, cmd=cmd, k=kind, dl=dl, sim=sim)))
        stats["sent"] += 1
        stats["sent_" + kind] += 1
        return rid, fut, dl

    async def call(c, step, ns_app, timeout_s):
        """One command, waited for until its deadline. Returns the reply status, or None if OpsAI gave up."""
        cmd, kind, sim = step
        rid, fut, dl = send(c, cmd.format(ns=ns_app[0], app=ns_app[1]), kind, sim, timeout_s)
        try:
            return await asyncio.wait_for(fut, timeout=(dl - time.time_ns()) / 1e9)
        except asyncio.TimeoutError:
            waiting.pop(rid, None)
            gave_up.add(rid)
            stats["gave_up"] += 1
            return None

    async def investigation():
        c = cluster(random.randrange(a.clusters))
        ns_app = random.choice(APPS)
        steps = [[s for s in TRIAGE], [SLOW_LOGS if random.random() < a.slow_share else LOGS_CMD]]
        if random.random() < a.autofix:
            steps += [[random.choice(FIXES)], [VERIFY]]
        for group in steps:
            await asyncio.sleep(random.uniform(*a.think))
            res = await asyncio.gather(*(call(c, s, ns_app, a.write_timeout if s[1] == "write" else a.read_timeout)
                                         for s in group))
            if any(r != "ok" for r in res):
                stats["to_oncall"] += 1    # no answer in time, or the command could not run: a human takes it
                return
        stats["finished"] += 1

    async def keep_open():
        # Hold --investigations open (x --storm-x during the storm), each ending with a new one.
        running = set()
        t0 = time.monotonic()
        while True:
            storm["on"] = a.storm_at <= time.monotonic() - t0 < a.storm_at + a.storm_for
            want = int(a.investigations * (a.storm_x if storm["on"] else 1))
            running = {t for t in running if not t.done()}
            for _ in range(want - len(running)):
                running.add(asyncio.get_running_loop().create_task(investigation()))
            await asyncio.sleep(0.05)

    async def send_requests():
        # One batch per partition every 2 ms, sent together: one round trip per tick, each cluster in order.
        async with producer:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batches = dict(pending)
                    pending.clear()
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))

    def on_reply(m, now):
        d = load(m.payload())
        rid = d["id"]
        if not rid.startswith(run + "-"):
            return                          # a reply to an earlier run
        if rid in seen:
            stats["dups"] += 1              # the same reply twice: counted once
            return
        seen.add(rid)
        sent_at.pop(rid, None)
        st = d["st"]
        stats["st_" + st] += 1
        if not d["held"]:                   # live traffic only: requests that waited in the log are counted apart
            back = (now - d["ns"]) / 1e6
            q_lat.add(d["q"])
            r_lat.add(back)
            rt_lat.add(d["q"] + back)
        w = waiting.pop(rid, None)
        if w is None:
            stats["late"] += 1              # OpsAI had already given up on this one
            return
        fut, dl = w
        if not fut.done():
            fut.set_result(st)
        if st == "ok":
            stats["in_time"] += 1

    async def read(pids):
        # Each reply partition in turn, from this run's start, then from our own position.
        c = await own_client()
        who = {p: Consumer.Single(f"opsai-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, REPLIES, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.Timestamp(start_ns // 1000))
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_reply(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        print(f"[opsai] run {run}: {a.investigations} investigations open over {a.clusters:,} clusters, "
              f"Auto Fix in {a.autofix:.0%}, timeouts {a.read_timeout:g} s (reads) and {a.write_timeout:g} s (writes)",
              flush=True)
        while True:
            await asyncio.sleep(1)
            q, r, rt = q_lat.flush(), r_lat.flush(), rt_lat.flush()
            now = time.time_ns()
            stats["missing"] = sum(1 for dl in sent_at.values() if now > dl + int(a.grace_s * 1e9))
            for k, v in stats.items():
                if k != "missing":
                    total[k] += v
            ok = "idle" if not rt["n"] else "PASS" if rt["p99"] < a.budget_ms else "FAIL"
            print(f"[opsai] {rt['n']:>6,} replies/s  request-to-agent p50 {q['p50']:5.1f}ms p99 {q['p99']:5.1f}ms  "
                  f"reply-to-opsai p50 {r['p50']:5.1f}ms p99 {r['p99']:5.1f}ms  "
                  f"lane round trip p50 {rt['p50']:5.1f}ms p99 {rt['p99']:5.1f}ms p99.9 {rt['p999']:5.1f}ms  "
                  f"{ok} (<{a.budget_ms}ms)  {'storm' if storm['on'] else 'steady'}", flush=True)
            print(f"[opsai] commands sent {stats['sent']:,} (writes {stats['sent_write']:,})  "
                  f"answered in time {stats['in_time']:,}  stopped at deadline {stats['st_timeout']:,}  "
                  f"expired, not run {stats['st_expired']:,}  answered after OpsAI gave up {stats['late']:,}  "
                  f"gave up {stats['gave_up']:,}  duplicates dropped {stats['dups']:,}  "
                  f"never answered {stats['missing']:,}  investigations finished {stats['finished']:,}, "
                  f"to on-call {stats['to_oncall']:,}", flush=True)
            print(f"[opsai] totals: sent {total['sent']:,}  answered in time {total['in_time']:,}  "
                  f"expired, not run {total['st_expired']:,}  stopped at deadline {total['st_timeout']:,}  "
                  f"answered after OpsAI gave up {total['late']:,}  duplicates dropped {total['dups']:,}  "
                  f"investigations finished {total['finished']:,}, to on-call {total['to_oncall']:,}", flush=True)
            stats.clear()

    gc.freeze()                             # keep start-up objects out of the collector's way
    parts = list(range(PARTITIONS[REPLIES]))
    await asyncio.gather(send_requests(), keep_open(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--clusters", type=int, default=2000, help="customer clusters with the OpsAI agent")
    p.add_argument("--investigations", type=int, default=300, help="investigations open at once")
    p.add_argument("--autofix", type=float, default=0.3, help="share of investigations in Auto Fix mode")
    p.add_argument("--slow-share", type=float, default=0.05, help="share of log reads that take seconds")
    p.add_argument("--think", type=float, nargs=2, default=(0.2, 1.0), help="seconds between steps (min max)")
    p.add_argument("--read-timeout", type=float, default=15, help="timeout_seconds sent with reads")
    p.add_argument("--write-timeout", type=float, default=30, help="timeout_seconds sent with writes")
    p.add_argument("--budget-ms", type=int, default=50, help="lane round trip p99 budget (both hops)")
    p.add_argument("--storm-x", type=float, default=1, help="investigations multiplier during an incident storm")
    p.add_argument("--storm-at", type=float, default=0, help="seconds after start")
    p.add_argument("--storm-for", type=float, default=60, help="seconds")
    p.add_argument("--grace-s", type=float, default=30, help="count a request never answered this long after its deadline")
    p.add_argument("--readers", type=int, default=1)
    p.add_argument("--poll-ms", type=float, default=1)
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
    os._exit(0)
