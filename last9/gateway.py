"""The ingestion side: gateways and receivers putting every metric sample on the streaming aggregation lane.

Plays the scrape and remote-write traffic of --clusters clusters in one process. Each cluster runs --services
services with --pods pods each, and every pod exports one histogram, http_requests_duration_seconds_bucket, per
route and bucket (le). Every series is scraped once per --scrape-s, spread evenly over the interval, and each
sample is published to the samples topic, keyed by shard key (cluster, metric, shard_label = service):
  * the series' labels, a per-series sequence, and the counter's value (the resulting cumulative count, as
    Prometheus and OTLP cumulative temporality send it, never only the increase), plus the scrape time;
  * a new series starts with its first value as a baseline (it adds nothing), as increase() treats it;
  * --reset-pct of samples are a counter reset: the process restarted and counts again from zero.
A rollout (--peak) adds a new generation of pods for --peak-for seconds: new series, the cardinality spike.

At-least-once on purpose: --resend-pct of samples are sent a second time later, as a remote-write client that
timed out waiting for an acknowledgement would. The aggregator (aggregator.py) must count each one once.

The check: for every timeslice the gateway keeps its own count of what each output group gained (cluster x
service x le), and publishes it to window-truth once the timeslice has closed, so the aggregator can compare.

Prints samples, live series and resets per second.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (DEFAULT_URL, PARTITIONS, SAMPLES, SERVICES, STREAM, TRUTH, buckets, cluster, ensure_topics,
                    msg, part, shard_key, window)

TICK = 0.01


async def main(a):
    client = IggyClient.from_connection_string(a.url)       # one connection for the samples producer
    await client.connect()
    await ensure_topics(client, SAMPLES, TRUTH)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await client.producer(STREAM, SAMPLES, topic_partitions_count=PARTITIONS[SAMPLES], mode=cfg)
    tclient = IggyClient.from_connection_string(a.url)      # and one for the truth producer
    await tclient.connect()
    truth_out = await tclient.producer(STREAM, TRUTH, topic_partitions_count=PARTITIONS[TRUTH])
    les = buckets(a.buckets)
    services = SERVICES[:a.services] if a.services <= len(SERVICES) else \
        SERVICES + [f"svc-{i}" for i in range(len(SERVICES), a.services)]
    slots = max(1, round(a.scrape_s / TICK))
    due = defaultdict(list)                  # slot -> series scraped in that slot of every interval
    truth = defaultdict(lambda: defaultdict(int))   # timeslice -> "cluster|service|le" -> increase
    recent = deque(maxlen=5000)              # recently sent samples, for the at-least-once resends
    stats = defaultdict(float)
    live = [0]

    def add_series(gen: int, pods: range):
        for ci in range(a.clusters):
            c = cluster(ci)
            for sv in services:
                q = part(shard_key(c, sv), PARTITIONS[SAMPLES])
                for p in pods:
                    pod = f"{sv}-g{gen}-{p:02d}"
                    for r in range(a.routes):
                        for le_i, le in enumerate(les):
                            # mean increase per scrape grows with the bucket, as cumulative buckets do
                            due[random.randrange(slots)].append({
                                "c": c, "sv": sv, "id": f"{pod}/r{r}", "le": le, "q": q, "seq": 0,
                                "v": random.randint(1_000, 50_000), "mean": 2 + 3 * le_i, "gen": gen})
                            live[0] += 1

    add_series(1, range(a.pods))
    gc.freeze()                              # keep start-up objects out of the collector's way
    rollout = {"on": False, "done": False}

    async def scrape():
        t0 = time.monotonic()
        nxt, tick = t0, 0
        async with producer:
            while True:
                t = time.monotonic() - t0
                if a.peak > 1 and not rollout["on"] and not rollout["done"] and t >= a.peak_at:
                    rollout["on"] = True     # a rollout: a new generation of pods, new series, both run a while
                    add_series(2, range(int(a.pods * (a.peak - 1))))
                if rollout["on"] and t >= a.peak_at + a.peak_for:
                    rollout["on"], rollout["done"] = False, True
                stats["peak"] = rollout["on"]
                now_ms = time.time_ns() // 1_000_000
                w = window(now_ms, a.timeslice_s)
                batches = defaultdict(list)
                keep = []
                for s in due.pop(tick % slots, []):
                    if s["gen"] == 2 and rollout["done"]:
                        live[0] -= 1         # the old generation's pods are gone after the rollout
                        continue
                    s["seq"] += 1
                    if s["seq"] > 1:         # the first sample of a series is its baseline
                        inc = random.randint(0, 2 * s["mean"])
                        if a.reset_pct and random.random() < a.reset_pct / 100 and s["v"] > inc:
                            s["v"] = inc     # the process restarted: the counter counts again from zero
                            stats["resets"] += 1
                        else:
                            s["v"] += inc
                        if inc:
                            truth[w][f"{s['c']}|{s['sv']}|{s['le']}"] += inc
                    b = msg(c=s["c"], sv=s["sv"], id=s["id"], le=s["le"], seq=s["seq"], v=s["v"], t=now_ms)
                    batches[s["q"]].append(SendMessage(b))
                    stats["sent"] += 1
                    if a.resend_pct and random.random() < a.resend_pct / 100 and recent:
                        q, old = random.choice(recent)   # a retry of an earlier sample: same series, same seq
                        batches[q].append(SendMessage(old))
                        stats["resent"] += 1
                    recent.append((s["q"], b))
                    keep.append(s)
                due[tick % slots].extend(keep)
                # One batch per partition, all sent at once: a shard key's samples stay in order, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                tick += 1
                nxt += TICK
                await asyncio.sleep(max(0.0, nxt - time.monotonic()))

    async def publish_truth():
        # Once a timeslice has closed (its end, the aggregator's grace, and a second more), publish what each
        # group gained in it, so the aggregator can check its own result.
        ts_ms = int(a.timeslice_s * 1000)
        async with truth_out:
            while True:
                await asyncio.sleep(0.2)
                now_ms = time.time_ns() // 1_000_000
                ready = [w for w in truth if (w + 1) * ts_ms + (a.grace_s + 1) * 1000 < now_ms]
                for w in sorted(ready):
                    g = truth.pop(w)
                    await truth_out.send([SendMessage(msg(w=w, g=dict(g)))])
                    stats["truth"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "rollout" if stats["peak"] else "steady"
            print(f"[gateway] {int(stats['sent']):>7,} samples/s ({phase})  live series {live[0]:,} in "
                  f"{a.clusters} clusters  resets {int(stats['resets']):,}  resent {int(stats['resent']):,}  "
                  f"timeslices reported {int(stats['truth'])}", flush=True)
            for k in ("sent", "resets", "resent"):
                stats[k] = 0

    await asyncio.gather(scrape(), publish_truth(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--clusters", type=int, default=4)
    p.add_argument("--services", type=int, default=20, help="services per cluster (the shard_label)")
    p.add_argument("--pods", type=int, default=10, help="pods per service")
    p.add_argument("--routes", type=int, default=10, help="routes per pod")
    p.add_argument("--buckets", type=int, default=12, help="histogram buckets per route, +Inf included")
    p.add_argument("--scrape-s", type=float, default=15, help="each series is scraped this often")
    p.add_argument("--timeslice-s", type=float, default=60, help="the aggregation timeslice (match aggregator.py)")
    p.add_argument("--grace-s", type=float, default=2, help="the aggregator's grace after a timeslice ends")
    p.add_argument("--resend-pct", type=float, default=0.5, help="share of samples sent twice (retries)")
    p.add_argument("--reset-pct", type=float, default=0.05, help="share of samples that are a counter reset")
    p.add_argument("--peak", type=float, default=1, help="pods multiplier during a rollout (new series)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
