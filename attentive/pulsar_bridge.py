"""Shadow shopping events from your Pulsar topics into the Apache Iggy decision lane, read-only.

Attaches to your Pulsar topics with its own subscription ("iggy-shadow"), so the producers, the
decision engine's KEY_SHARED subscription and every other subscription are untouched. Each event
is written to the subscriber-events topic, keyed by subscriber, with a per-subscriber sequence
number. Then run decision.py and send_audit.py against it and compare their texts and timings
with what your engine decided.

A Pulsar subscription keeps a backlog while its consumer is away: delete "iggy-shadow"
(pulsar-admin topics unsubscribe) when the shadow run ends.

Needs: pip install pulsar-client apache-iggy==0.9.0
Map your records in to_event(): the kit needs the subscriber ID, the brand, the event name and its time.
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

import pulsar
from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS_TOPIC, STREAM, connect, ensure_topic, msg, part


def to_event(key: str, value: bytes):
    """Return (subscriber id, brand id, event name, event time ns) from one of your records, or None to skip it."""
    d = json.loads(value)
    sub = d.get("subscriberId") or d.get("subscriber_id") or key
    name = d.get("type") or d.get("eventType")
    if not (sub and name):
        return None
    return str(sub), str(d.get("companyId") or d.get("brand_id") or ""), str(name), int(d.get("ts_ns") or time.time_ns())


async def main(a):
    iggy = await connect(a.url)
    await ensure_topic(iggy, STREAM, EVENTS_TOPIC, a.partitions)
    parts = (await iggy.get_topic(STREAM, EVENTS_TOPIC)).partitions_count
    client = pulsar.Client(a.service_url, **({"authentication": pulsar.AuthenticationToken(a.token)} if a.token else {}))
    consumer = client.subscribe(a.pulsar_topics, subscription_name="iggy-shadow",
                                consumer_type=pulsar.ConsumerType.Failover,     # one reader, in order per partition
                                initial_position=pulsar.InitialPosition.Latest)
    seq, stats = defaultdict(int), defaultdict(int)
    mark = time.monotonic()
    while True:
        batch = defaultdict(list)
        for _ in range(a.batch):
            try:
                rec = consumer.receive(timeout_millis=5)
            except pulsar.Timeout:
                break
            ev = to_event(rec.partition_key(), rec.data())
            consumer.acknowledge(rec)                      # acknowledges on the iggy-shadow subscription only
            if ev is None:
                continue
            sub, brand, name, ns = ev
            seq[sub] += 1                                  # per-subscriber order as read; restart the bridge = new run
            batch[part(sub, parts)].append(SendMessage(msg(ns=ns, u=sub, b=brand, e=name, seq=seq[sub])))
            stats["copied"] += 1
        if batch:
            await asyncio.gather(*(iggy.send_messages(STREAM, EVENTS_TOPIC, Partitioning.partition_id(pid), ms)
                                   for pid, ms in batch.items()))
        if time.monotonic() - mark >= 1:
            print(f"[bridge] {stats['copied']:,} shopping events/s copied into {STREAM}/{EVENTS_TOPIC}", flush=True)
            stats.clear()
            mark = time.monotonic()
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--service-url", required=True, help="pulsar+ssl://… of your cluster")
    p.add_argument("--pulsar-topics", nargs="+", required=True, help="the topics the decision engine reads")
    p.add_argument("--token", default=None, help="JWT for token authentication, if you use it")
    p.add_argument("--partitions", type=int, default=64, help="partitions for a new events topic")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
