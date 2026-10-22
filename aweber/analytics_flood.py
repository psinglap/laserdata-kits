"""The neighbour: Imbi's existing analytics writes on the same server, the ClickHouse sink path from ADR 0019.

Publishes rows the way imbi.common.iggy does today: one partition per topic, partition id 0, a JSON row per
message. Most of the volume is `events/gateway` (every webhook delivery, with its headers and payload, so rows are
bulky); the rest is `operations_log/deployments`. Rates and row sizes are illustrative; set them to yours.

Use it to check that a backfill or a webhook burst on the analytics streams does not move the notifications lane's
p99 (watch notify.py while this runs). One batch per topic per 10 ms tick, sized to what the tick owes, so the
neighbour itself doesn't flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import ANALYTICS, DEFAULT_URL, PARTITIONS, ensure, msg, project


async def main(a):
    prods = {}
    for stream, topic in ANALYTICS.items():
        c = IggyClient.from_connection_string(a.url)      # one connection per producer
        await c.connect()
        await ensure(c, stream, topic)
        prods[stream] = await c.producer(stream, topic, topic_partitions_count=PARTITIONS[topic],
                                         mode=DirectProducerConfig(batch_length=5000,
                                                                   linger_time=timedelta(milliseconds=5)))
    body = os.urandom(a.size // 2).hex()    # stands in for a webhook delivery's payload
    if a.at:
        print(f"[analytics] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    share = {"events": 0.85, "operations_log": 0.15}
    due, t0, end = time.monotonic(), time.monotonic(), time.monotonic() + (a.duration or 1e12)
    owed, last, sent = 0.0, time.monotonic(), 0
    while time.monotonic() < end:
        now = time.monotonic()
        owed += a.rate * (now - last)
        last, n = now, int(owed)
        owed -= n
        sends = []
        for stream, sh in share.items():
            k = round(n * sh)
            if not k:
                continue
            if stream == "events":
                ms = [SendMessage(msg(project_id=project(random.randrange(2000)), type="webhook",
                                      integration="github", metadata={"webhook_id": "wh-1"}, payload=body, version=0))
                      for _ in range(k)]
            else:
                ms = [SendMessage(msg(project_id=project(random.randrange(2000)), environment_slug="production",
                                      entry_type="Deployed", description="deploy", recorded_by="imbi"))
                      for _ in range(k)]
            sends.append(prods[stream].send_with_partitioning(ms, Partitioning.partition_id(0)))
            sent += k
        if sends:
            await asyncio.gather(*sends)
        if now - t0 >= 1:
            print(f"[analytics] {sent:,} rows/s on events/gateway and operations_log/deployments "
                  f"(~{sent * 0.85 * a.size / 1e6:.1f} MB/s of webhook rows)", flush=True)
            sent, t0 = 0, now
        due += 0.01
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[analytics] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--rate", type=float, default=2000, help="analytics rows a second")
    ap.add_argument("--size", type=int, default=2048, help="bytes of payload per webhook row")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
