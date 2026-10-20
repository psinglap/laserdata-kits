"""Agents reconnecting after an outage: every buffered batch arrives at once.

Simulates --agents hosts that could not reach the collectors for --minutes and now flush what they
held, as fast as the collectors accept it, on the same otlp_logs topic. Use it to see the log absorb
the burst: collectors.py keeps accepting live batches at the same latency, writer.py drains the
backlog at ClickHouse's pace, and verify.py finds nothing missing.
"""
import argparse
import asyncio
import gc
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, LOGS, STREAM, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    producer = await client.producer(STREAM, LOGS, topic_partitions_count=a.partitions,
                                     mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(milliseconds=2)))
    batches = int(a.agents * a.minutes * 60 * a.batches_per_s_per_agent)
    body = "GET /api/v1/orders status=200 duration_ms=42 (buffered on the host during the outage)"
    print(f"[reconnect] {a.agents} agents flush {a.minutes} min of buffered logs: {batches:,} batches "
          f"x {a.records} records", flush=True)
    gc.freeze()
    sent, t0, last = 0, time.monotonic(), time.monotonic()
    seq = [0] * a.agents
    async with producer:
        while sent < batches:
            ms = []
            for _ in range(min(a.per_tick, batches - sent)):
                i = random.randrange(a.agents)
                seq[i] += 1
                old = time.time_ns() - a.minutes * 60 * 10**9
                ms.append(SendMessage(msg(t=f"agent-{i:03d}", seq=seq[i], svc="host-metrics",
                                          recs=[[old, "INFO", body]] * a.records)))
            await producer.send_with_partitioning(ms, Partitioning.balanced())   # one batch per tick
            sent += len(ms)
            if time.monotonic() - last >= 1:
                print(f"[reconnect] sent {sent:,} of {batches:,} buffered batches", flush=True)
                last = time.monotonic()
            await asyncio.sleep(0.01)
    print(f"[reconnect] done: {sent:,} batches in {time.monotonic() - t0:.1f}s", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--agents", type=int, default=20)
    p.add_argument("--minutes", type=float, default=30, help="how long the agents could not reach the collectors")
    p.add_argument("--batches-per-s-per-agent", type=float, default=1)
    p.add_argument("--records", type=int, default=100)
    p.add_argument("--per-tick", type=int, default=500, help="batches per send, one send every 10 ms")
    p.add_argument("--partitions", type=int, default=16)
    asyncio.run(main(p.parse_args()))
