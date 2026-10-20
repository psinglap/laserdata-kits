"""A fleet of call workers: every provider outcome onto the lane, and every health verdict applied before the next turn.

Plays --workers call workers in one process, hosting --calls live calls between them. Each call stays on one
worker, in one region, with a primary and a fallback provider per stage (speech-to-text, LLM, voice). On each
turn (illustrative gaps of 3-9 s, see common.next_gap) the call uses one provider per stage, and the worker
publishes one record per provider call to provider-outcomes, keyed by stage:provider:region:
  * worker, the worker's sequence for that key, call, ok or failed, time to first byte, and whether it was a probe.

Provider incidents are planted here (the kit's ground truth): every --incident-every seconds one
stage:provider:region starts failing --fail-rate of its requests for --incident-for seconds.

Each worker reads provider-health on its own connection and consumer group. DEGRADED for a key moves new turns
that would use it to the fallback (a --probe-share still go to the primary, so the evaluator sees recovery);
OK moves them back.

The business number: turns sent to a failing provider, and of those, how many went out after the evaluator had
already marked it degraded, because the verdict had not reached that worker yet. Prints it every second, with
what a --compare-ms lane would have let through, and verdict-to-workers p50/p99: the second hop of the loop.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, DEGRADED, HEALTH, MEAN_GAP, OUTCOMES, PARTITIONS, PROVIDERS, REGIONS, STAGES,
                    STREAM, TTFB_MS, Window, call_id, ensure_topics, key, load, msg, next_gap, part)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), OUTCOMES, HEALTH)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, OUTCOMES, topic_partitions_count=PARTITIONS[OUTCOMES],
                                                   mode=cfg)
    start_ns = time.time_ns()
    rnd = random.Random(7)
    workers = [f"worker-{i:02d}" for i in range(a.workers)]
    calls = []                              # (call id, worker, region, {stage: (primary, fallback)})
    for i in range(a.calls):
        route = {}
        for s in STAGES:
            p, f = PROVIDERS[s] if rnd.random() < 0.7 else PROVIDERS[s][::-1]
            route[s] = (p, f)
        calls.append((call_id(i), workers[i % a.workers], REGIONS[i % len(REGIONS)], route))
    every_key = [key(s, p, r) for s in STAGES for p in PROVIDERS[s] for r in REGIONS]
    pid = {k: part(k, PARTITIONS[OUTCOMES]) for k in every_key}
    targets = [key(s, PROVIDERS[s][0], r) for r in REGIONS for s in STAGES]   # incidents hit primaries in turn
    rnd.shuffle(targets)

    now = time.monotonic()
    due = [(now + random.uniform(0, next_gap()), i) for i in range(a.calls)]
    heapq.heapify(due)
    state = {w: {} for w in workers}        # worker -> {key: degraded or ok}, as that worker knows it
    vns = {w: {} for w in workers}          # worker -> {key: ns of the verdict it applied}
    sent = {w: defaultdict(deque) for w in workers}   # worker -> {key: ns of recent non-probe turns sent to it}
    cmp_until = {w: {} for w in workers}    # worker -> {key: verdict ns + compare window}
    seq = defaultdict(int)                  # (worker, key) -> sequence
    incident = {"k": None, "onset": 0, "end": 0.0, "got": {}, "verdict_ns": 0, "hit": 0}
    lat, stats = Window(), defaultdict(float)

    def reader(w):
        async def on_health(m):
            d = load(m.payload())
            k = d["k"]
            if d["ns"] <= vns[w].get(k, 0):
                return                      # older or repeated verdict: the newest wins
            vns[w][k] = d["ns"]
            state[w][k] = d["state"]
            live = d["ns"] > start_ns
            if live:
                lat.add((time.time_ns() - d["ns"]) / 1e6)   # live verdicts only, not ones replayed at start-up
            if d["state"] != DEGRADED or not live:
                return
            # Turns this worker sent to the provider after the verdict existed, before it arrived here.
            blind = sum(1 for t in sent[w][k] if t >= d["ns"])
            stats["blind"] += blind
            stats["at_compare"] += blind
            cmp_until[w][k] = d["ns"] + int(a.compare_ms * 1e6)
            if incident["k"] == k:
                incident["got"][w] = time.time_ns()
                incident["verdict_ns"] = incident["verdict_ns"] or d["ns"]
                if len(incident["got"]) == len(workers):
                    last = max(incident["got"].values())
                    print(f"[calls] incident {k}: onset to verdict {(incident['verdict_ns'] - incident['onset']) / 1e6:.0f} ms, "
                          f"verdict to all {len(workers)} workers {(last - incident['verdict_ns']) / 1e6:.1f} ms, "
                          f"turns that hit it first {incident['hit']:,}", flush=True)
                    stats["incidents"] += 1
        return on_health

    readers = []
    for w in workers:
        readers.append((await (await own_client()).consumer_group(
            f"{w}-{os.getpid()}", STREAM, HEALTH, polling_strategy=PollingStrategy.First(), batch_length=1000,
            auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500)), reader(w)))
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def turn_loop():
        t0 = tick = time.monotonic()
        n_inc = 0
        async with producer:
            while True:
                now = time.monotonic()
                el = now - t0
                x = a.peak if a.peak_at <= el < a.peak_at + a.peak_for else 1
                if incident["k"] and now >= incident["end"]:
                    incident["k"] = None
                start_at = a.incident_at + n_inc * a.incident_every
                if a.incident_every and el >= start_at and not incident["k"]:
                    incident.update(k=targets[n_inc % len(targets)], onset=time.time_ns(), end=now + a.incident_for,
                                    got={}, verdict_ns=0, hit=0)
                    n_inc += 1
                now_ns = time.time_ns()
                batches = defaultdict(list)
                while due and due[0][0] <= now:
                    _, i = heapq.heappop(due)
                    c, w, region, route = calls[i]
                    for s in STAGES:
                        p, f = route[s]
                        k = key(s, p, region)
                        probe = False
                        if cmp_until[w].get(k, 0) > now_ns:
                            stats["at_compare"] += 1    # a slower lane would still be sending this turn to k
                        if state[w].get(k) == DEGRADED:
                            if random.random() < a.probe_share:
                                probe = True
                            else:
                                k = key(s, f, region)
                        failing = k == incident["k"]
                        ok = not (failing and random.random() < a.fail_rate)
                        if failing and not probe:
                            stats["hit"] += 1
                            if not vns[w].get(k) or state[w].get(k) != DEGRADED:
                                incident["hit"] += 1
                        if not probe:
                            dq = sent[w][k]
                            dq.append(now_ns)
                            while dq and dq[0] < now_ns - 10_000_000_000:
                                dq.popleft()
                        lo, hi = TTFB_MS[s]
                        seq[(w, k)] += 1
                        batches[pid[k]].append(SendMessage(msg(
                            k=k, w=w, seq=seq[(w, k)], call=c, ok=ok, probe=probe,
                            ttfb=round(random.uniform(lo, hi)) if ok else a.timeout_ms)))
                        stats["outcomes"] += 1
                    stats["turns"] += 1
                    heapq.heappush(due, (now + next_gap() / x, i))
                # One batch per partition, all sent at once: per-key order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["peak"] = x > 1
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[calls] {a.workers} call workers, {a.calls:,} live calls, ~{a.calls / MEAN_GAP:,.0f} turns/s, "
              f"incident every {a.incident_every:g} s for {a.incident_for:g} s", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "surge" if stats["peak"] else "steady"
            print(f"[calls] {int(stats['turns']):>7,} turns/s ({phase})  outcomes {int(stats['outcomes']):,}  "
                  f"turns on a failing provider {int(stats['hit']):,}, after its verdict was out "
                  f"{int(stats['blind']):,} (at {a.compare_ms:g} ms it would be {int(stats['at_compare']):,})  "
                  f"verdict-to-workers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("turns", "outcomes", "hit", "blind", "at_compare"):
                stats[k] = 0

    await asyncio.gather(turn_loop(), report(), *(c.consume_messages(cb, asyncio.Event()) for c, cb in readers))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--workers", type=int, default=8, help="call workers, each with its own health reader")
    p.add_argument("--calls", type=int, default=20_000, help="live calls across all workers")
    p.add_argument("--incident-at", type=float, default=15, help="seconds after start of the first provider incident")
    p.add_argument("--incident-every", type=float, default=10, help="seconds between incidents (0 = none)")
    p.add_argument("--incident-for", type=float, default=5, help="seconds each incident lasts")
    p.add_argument("--fail-rate", type=float, default=0.6, help="share of requests a failing provider fails")
    p.add_argument("--timeout-ms", type=int, default=1500, help="time a failed request costs the turn (illustrative)")
    p.add_argument("--probe-share", type=float, default=0.03, help="share of turns still sent to a degraded primary")
    p.add_argument("--compare-ms", type=float, default=1000, help="also count turns a lane this slow would let through")
    p.add_argument("--peak", type=float, default=1, help="turn-rate multiplier during a campaign surge")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
