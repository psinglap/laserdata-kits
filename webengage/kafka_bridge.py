"""Shadow tracked events from a Kafka topic into the Apache Iggy trip lane, read-only.

WebEngage's internal event pipeline is not public; its job posts ask for Kafka or RabbitMQ
experience. If your tracked events pass through a Kafka topic, this bridge joins it as its own
consumer group ("iggy-shadow"), so producers and every existing consumer are untouched. It keeps
only the events your Contextual Journeys use (their entry, exit and wait events), reads the context
attribute from each one's eventData, and writes it to the tracked-events topic, keyed by user, with
a per-user sequence number. Then run journey_engine.py and send_audit.py against it and compare their
reminders with what your journeys sent.

Records are read in the shape of your /events REST API body:
  {"userId": "...", "anonymousId": "...", "eventName": "Policy Renewed",
   "eventTime": "2026-10-22T10:15:30+0530", "eventData": {"policy_id": "POL-1042"}}
Map other shapes in to_event(). Name your own journeys' events and context attributes with --context,
for example: --context "Renewal Due=policy_id" --context "Policy Renewed=policy_id"
(the attribute names may differ between events, as long as each carries the same value).

Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer as KafkaConsumer

from common import DEFAULT_URL, EVENTS_TOPIC, JOURNEYS, ROLE, STREAM, connect, ensure_topic, msg, part


def to_event(value: bytes, attr_of: dict):
    """Return (user id, event name, context value, event time ns) from one of your records, or None to skip it."""
    d = json.loads(value)
    name = d.get("eventName")
    if name not in attr_of:
        return None
    user = d.get("userId") or d.get("anonymousId")
    data = d.get("eventData") or {}
    if isinstance(data, str):
        data = json.loads(data)
    ctx = data.get(attr_of[name])
    if not (user and ctx is not None):
        return None
    ns = time.time_ns()
    if d.get("eventTime"):
        try:
            ns = int(datetime.strptime(d["eventTime"], "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1e9)
        except ValueError:
            pass
    return str(user), name, str(ctx), ns


async def main(a):
    attr_of = {n: JOURNEYS[j]["ctx"] for n, (j, _) in ROLE.items()}       # the kit's journeys by default
    if a.context:
        attr_of = dict(kv.split("=", 1) for kv in a.context)
    iggy = await connect(a.url)
    await ensure_topic(iggy, STREAM, EVENTS_TOPIC, a.partitions)
    parts = (await iggy.get_topic(STREAM, EVENTS_TOPIC)).partitions_count
    kafka = KafkaConsumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow",
                           "auto.offset.reset": "latest", "enable.auto.commit": True,
                           **dict(kv.split("=", 1) for kv in a.X)})
    kafka.subscribe(a.kafka_topics)
    seq, stats = defaultdict(int), defaultdict(int)
    mark = time.monotonic()
    while True:
        batch = defaultdict(list)
        for rec in kafka.consume(num_messages=5000, timeout=0.005):
            if rec.error():
                continue
            ev = to_event(rec.value(), attr_of)
            if ev is None:
                continue
            user, name, ctx, ns = ev
            seq[user] += 1                               # per-user order as read; restart the bridge = new run
            batch[part(user, parts)].append(SendMessage(msg(ns=ns, u=user, e=name, c=ctx, seq=seq[user])))
            stats["copied"] += 1
        if batch:
            await asyncio.gather(*(iggy.send_messages(STREAM, EVENTS_TOPIC, Partitioning.partition_id(pid), ms)
                                   for pid, ms in batch.items()))
        if time.monotonic() - mark >= 1:
            print(f"[bridge] {stats['copied']:,} journey events/s copied into {STREAM}/{EVENTS_TOPIC}", flush=True)
            stats.clear()
            mark = time.monotonic()
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--kafka-topics", nargs="+", required=True, help="the topics your tracked events pass through")
    p.add_argument("--context", action="append", default=[], metavar="Event Name=attribute",
                   help="an event your journeys use and the eventData attribute holding its context")
    p.add_argument("--partitions", type=int, default=64, help="partitions for a new events topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka consumer setting")
    asyncio.run(main(p.parse_args()))
