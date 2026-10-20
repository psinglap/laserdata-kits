"""A heavy API-log stream on the same broker: request and response logs on their own topic, beside the lane.

Hyperswitch publishes API logs, connector call logs and webhook logs to Kafka for ClickHouse
(topics such as hyperswitch-api-log-events), and Juspay's blog counts about 10 API calls per
payment. That traffic is large and latency-tolerant. This script plays a slice of it, so you can
check it does not move the outcome lane's p99 (watch guard.py while it runs). It sends one batch
per tick, spread across partitions, as a real bulk producer would.
"""
import argparse
import asyncio
import gc
import os
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import API_LOGS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, API_LOGS)
    p = await client.producer(STREAM, API_LOGS, topic_partitions_count=PARTITIONS[API_LOGS],
                              mode=DirectProducerConfig(batch_length=2000, linger_time=timedelta(milliseconds=5)))
    blob = os.urandom(a.size // 2).hex()        # stands in for a logged request and response body
    per_s = a.mbps * 1e6 / a.size
    await asyncio.sleep(max(0.0, a.at))
    print(f"[api-logs] starting: ~{a.mbps:g} MB/s on '{API_LOGS}'", flush=True)
    gc.freeze()
    due = t0 = start = last = time.monotonic()
    sent, owed = 0, 0.0
    async with p:
        while not a.duration or time.monotonic() - start < a.duration:
            now = time.monotonic()
            owed += per_s * (now - last)        # by elapsed time, so a slow send means a bigger next batch
            last, n = now, int(owed)
            owed -= n
            if n:
                await p.send_with_partitioning([SendMessage(msg(kind="api_log", v=blob)) for _ in range(n)],
                                               Partitioning.balanced())
            sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[api-logs] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[api-logs] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=20, help="megabytes per second")
    ap.add_argument("--size", type=int, default=2048, help="bytes per message")
    ap.add_argument("--at", type=float, default=0, help="start this many seconds in")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run; 0 runs until stopped")
    asyncio.run(main(ap.parse_args()))
