"""OpenTelemetry gateway collectors writing log events into Apache Iggy, keyed by source.

Every source (one pod of one service in one region) keeps its own sequence number, so verify.py can prove that every
event reached ClickHouse exactly once. Sends one batch per partition per 10 ms tick, all partitions at once.
--spike-x multiplies the rate for --spike-for seconds from --spike-at (an incident or a deploy wave).
"""
import argparse
import asyncio
import gc
import random
import time
import uuid
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, LOGS, PARTITIONS, SEVERITY, STATE, STREAM, Window, ensure_topics, event, part, source

BODIES = {
    "clickhouse-server": ["Merged 6 parts: 0_1_6_1 -> 0_1_6_2", "Selected 2 parts from 0_7_7_0 to 0_8_8_0",
                          "Query finished: read 1.2M rows, 96 MiB in 0.31 s", "Removing part from filesystem 0_3_3_0"],
    "clickhouse-keeper": ["Created snapshot at log index 5120000", "Session 0x2a established", "Compacted log up to 5119000"],
    "ingress-proxy": ["GET /?query=SELECT 200 41ms", "POST /?query=INSERT 200 12ms", "upstream connect timeout, retrying"],
    "k8s-operator": ["reconcile ok", "scaled replica set 3 -> 4", "pod ready after 18 s"],
    "autoscaler": ["recommendation: 16 GiB -> 24 GiB", "cooldown active, skipping", "memory pressure 0.82"],
    "backup-agent": ["backup started", "uploaded 512 MiB to object storage", "backup finished in 92 s"],
    "billing-usage": ["usage sample written", "compute minutes rolled up", "storage sample written"],
}


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, LOGS, STATE)
    parts = PARTITIONS[LOGS]
    srcs = [source(i) for i in range(a.sources)]
    keys = [f"{r}/{s}/{p}" for r, s, p in srcs]
    pids = [part(k, parts) for k in keys]
    sev_names, sev_w = zip(*SEVERITY)
    seq = [0] * a.sources
    sent, send_ms, total = 0, Window(), 0
    gc.freeze()                                    # no collector pauses in the hot path once started

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
        by_part = defaultdict(list)
        ns = time.time_ns()
        for i in random.choices(range(a.sources), k=n):
            seq[i] += 1
            r, svc, pod = srcs[i]
            body = random.choice(BODIES[svc])
            by_part[pids[i]].append(SendMessage(event(ns, keys[i], r, svc, pod, seq[i],
                                                      random.choices(sev_names, sev_w)[0], body, uuid.uuid4().hex)))
        if by_part:
            t0 = time.perf_counter()
            await asyncio.gather(*(client.send_messages(STREAM, LOGS, Partitioning.partition_id(p), ms)
                                   for p, ms in by_part.items()))
            send_ms.add((time.perf_counter() - t0) * 1000)
            sent += n
            total += n
        if now - last_report >= 1:
            last_report += 1
            w = send_ms.flush()
            print(f"[collectors] {sent:>7,} events/s from {a.sources:,} sources  send p50 {w['p50']:5.1f}ms "
                  f"p99 {w['p99']:5.1f}ms  total {total:,}", flush=True)
            sent = 0
        next_t += tick                             # schedule from the due time, so a slow tick doesn't drift
        await asyncio.sleep(max(0.0, next_t - time.monotonic()))
    print(f"[collectors] done: sent {total:,} events from {a.sources:,} sources", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=20000, help="events per second, all sources together")
    p.add_argument("--sources", type=int, default=5000, help="pods writing logs (region/service/pod)")
    p.add_argument("--duration", type=float, default=600)
    p.add_argument("--spike-x", type=float, default=1, help="rate multiplier during the spike")
    p.add_argument("--spike-at", type=float, default=1e9)
    p.add_argument("--spike-for", type=float, default=0)
    asyncio.run(main(p.parse_args()))
