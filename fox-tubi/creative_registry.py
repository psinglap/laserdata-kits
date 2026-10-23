"""The creative registry, fed by the creative-seen lane.

Reads every partition of creative-seen directly, on --readers connections of its own, from the start of the
log, tracking its own position (no consumer group: a group reader visits one partition per poll, so with 16
partitions a record waits for its partition's turn). Each record says that one ad server node met a creative
it holds no verdict for: node, creative, DSP, and a per-node sequence, so a replayed or repeated record is
dropped and counted once.

Sightings are keyed by creative, so all sightings of one creative land on one partition and one reader. The
first starts the creative's job (transcode, loudness, standards check, brand recognition); every later
sighting from another node is absorbed instead of starting the job again. The job here is a timer of about
--process-s seconds; your transcoding and brand recognition pipeline goes in its place.

When a job finishes, its verdict goes out on creative-ready: ready, with canonical brand and ad category (for
competitive separation and brand reporting), or held. Every ad server node reads it (ad_servers.py).

At start it publishes the verdicts for the --catalog creatives already approved (a start-up snapshot, flagged
so it is not timed). Prints creative-seen-to-registry p50/p99/p99.9 every second, with PASS/FAIL against
--budget-ms, and the jobs started, sightings absorbed and verdicts sent.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (DEFAULT_URL, PARTITIONS, READY, SEEN, STREAM, Window, attributes, creative_id, ensure_topics,
                    load, msg)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await own_client()
    await ensure_topics(admin, SEEN, READY)
    parts = list(range((await admin.get_topic(STREAM, SEEN)).partitions_count))
    out = await (await own_client()).producer(STREAM, READY, topic_partitions_count=PARTITIONS[READY])
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # node -> last sequence applied
    state = {}                              # creative -> "processing" | "ready" | "held"
    jobs = []                               # (due, creative) heap of running jobs
    pending = []                            # verdicts waiting for the next send
    caught_up = [False]                     # a new reader first replays sightings from before it joined
    rng = random.Random(11)

    # Start-up snapshot: the creatives already approved before this run, one verdict each, not timed.
    for n in range(a.catalog):
        c = creative_id(n)
        v = attributes(c)
        state[c] = v["status"]
        pending.append(SendMessage(msg(c=c, seq=1, snap=1, **v)))
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_seen(m, now):
        s = load(m.payload())
        age = (now - s["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        if s["seq"] <= seq.get(s["x"], 0):
            stats["dropped"] += 1           # replayed or repeated: counted once
            return
        seq[s["x"]] = s["seq"]
        stats["sightings"] += 1
        c = s["c"]
        st = state.get(c)
        if st is None:                      # first sighting anywhere: start the one job for this creative
            state[c] = "processing"
            heapq.heappush(jobs, (time.monotonic() + a.process_s * rng.lognormvariate(0, 0.3), c))
            stats["jobs"] += 1
        elif st == "processing":
            stats["absorbed"] += 1          # another node met it while its job runs: no second job
        else:
            stats["late"] += 1              # verdict already out, still on its way to that node

    async def read(pids):
        # Each partition in turn, from the start of the log, then from our own position; nothing is stored on
        # the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, SEEN, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_seen(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def finish_jobs():
        # Jobs end on their own timers; check every 5 ms from the due time, not from "now".
        due = time.monotonic()
        while True:
            due += 0.005
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            now = time.monotonic()
            while jobs and jobs[0][0] <= now:
                _, c = heapq.heappop(jobs)
                v = attributes(c)
                state[c] = v["status"]
                pending.append(SendMessage(msg(c=c, seq=1, **v)))
                stats["ready" if v["status"] == "ready" else "held"] += 1

    async def send_verdicts():
        # Verdicts come in bursts; one batch every 2 ms, never one send per verdict.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:a.batch]
                    del pending[:len(batch)]
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of sightings arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier sightings this second, "
                      f"verdicts sent {int(stats['sent']):,}", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>6,} sightings/s  seen-to-registry p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"jobs started {int(stats['jobs']):,}  absorbed {int(stats['absorbed'] + stats['late']):,}  "
                  f"in progress {len(jobs):,}  verdicts ready {int(stats['ready']):,} held {int(stats['held']):,}  "
                  f"sent {int(stats['sent']):,}  repeats dropped {int(stats['dropped']):,}", flush=True)
            stats.clear()

    await asyncio.gather(finish_jobs(), send_verdicts(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="registry")
    p.add_argument("--budget-ms", type=int, default=50, help="seen-to-registry p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the creative-seen partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--catalog", type=int, default=6000, help="creatives already approved at start (same as ad_servers.py)")
    p.add_argument("--process-s", type=float, default=8, help="typical seconds for transcode and brand recognition")
    asyncio.run(main(p.parse_args()))
