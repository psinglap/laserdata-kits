"""Gateway collectors: accept OTLP log batches from many tenants and append each one to the log.

Stands in for the receive side of a collector fleet. Every export batch an agent sends becomes one
message on the otlp_logs topic, keyed by tenant, with a per-tenant sequence number and the moment it
was accepted. The collector's job ends when Apache Iggy acknowledges the batch, so agents are never
held up by ClickHouse; writer.py drains the log into ClickHouse at its own pace.

Prints accepted batches and records per second and the accept latency (send to acknowledgement),
which is what agents feel. With --duration it stops on its own and writes the totals to --totals,
so verify.py can count every accepted record in ClickHouse.
"""
import argparse
import asyncio
import gc
import json
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, LOGS, STREAM, Window, msg, partition

SEVERITY = ["INFO"] * 8 + ["WARN", "ERROR"]
BODIES = [f"{verb} {path} status={code} duration_ms={ms} trace_id={random.getrandbits(64):016x}"
          for verb, path, code, ms in ((random.choice(["GET", "POST", "PUT"]),
                                        random.choice(["/api/v1/orders", "/api/v1/cart", "/checkout", "/login", "/search"]),
                                        random.choice([200, 200, 200, 201, 404, 500]), random.randint(1, 900))
                                       for _ in range(500))]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    producer = await client.producer(STREAM, LOGS, topic_partitions_count=a.partitions,   # replicated durability by default
                                     mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=500)))
    tenants = [f"tenant-{i:04d}" for i in range(a.tenants)]
    weights = [1 / (i + 1) ** 0.8 for i in range(a.tenants)]           # a few large tenants, a long tail of small ones
    services = [f"svc-{i:02d}" for i in range(a.services)]
    part = {t: partition(t, a.partitions) for t in tenants}
    seq, totals, accept = defaultdict(int), {"batches": 0, "records": 0}, Window()
    gc.freeze()                                                        # keep start-up objects out of the collector's way

    def export_batch(tenant):
        seq[tenant] += 1
        now = time.time_ns()
        recs = [[now - random.randint(0, 2_000_000_000), random.choice(SEVERITY), random.choice(BODIES)]
                for _ in range(a.records)]
        return SendMessage(msg(t=tenant, seq=seq[tenant], svc=random.choice(services), recs=recs))

    async def send(pid, batch):
        t0 = time.perf_counter()
        await producer.send_with_partitioning(batch, Partitioning.partition_id(pid))
        ms = (time.perf_counter() - t0) * 1000
        for _ in batch:
            accept.add(ms)
        totals["batches"] += len(batch)
        totals["records"] += len(batch) * a.records

    async def run():
        t0 = due = time.monotonic()
        carry = 0.0
        while not a.duration or time.monotonic() - t0 < a.duration:
            t = time.monotonic() - t0
            mult = a.burst if a.burst_at and a.burst_at <= t < a.burst_at + a.burst_for else 1
            carry += a.batches_per_s * mult / 100
            n, carry = int(carry), carry - int(carry)
            by_part = defaultdict(list)
            for tenant in random.choices(tenants, weights, k=n):
                by_part[part[tenant]].append(export_batch(tenant))
            # one batch per partition, all sent at once: per-tenant order, about one round trip per tick
            await asyncio.gather(*(send(pid, b) for pid, b in by_part.items()))
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        last = dict(totals)
        t0 = time.monotonic()
        while True:
            await asyncio.sleep(1)
            r = accept.flush()
            b, rec = totals["batches"] - last["batches"], totals["records"] - last["records"]
            last = dict(totals)
            print(f"[collectors] t={time.monotonic() - t0:4.0f}s {b:>6,} batches/s {rec:>9,} records/s  "
                  f"accept p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  accepted {totals['records']:,} records", flush=True)

    reporter = asyncio.create_task(report())
    async with producer:
        await run()
    reporter.cancel()
    with open(a.totals, "w") as f:
        json.dump(totals, f)
    print(f"[collectors] done: accepted {totals['batches']:,} batches, {totals['records']:,} records -> {a.totals}", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--tenants", type=int, default=200)
    p.add_argument("--services", type=int, default=40)
    p.add_argument("--batches-per-s", type=float, default=1000, help="OTLP export batches accepted per second")
    p.add_argument("--records", type=int, default=100, help="log records per export batch")
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--burst", type=float, default=1.0, help="multiplier during the burst")
    p.add_argument("--burst-at", type=float, default=0)
    p.add_argument("--burst-for", type=float, default=60)
    p.add_argument("--duration", type=float, default=0, help="stop after this many seconds (0 = run until stopped)")
    p.add_argument("--totals", default="collectors.totals.json")
    asyncio.run(main(p.parse_args()))
