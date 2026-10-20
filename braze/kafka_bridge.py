"""Shadow engagement events from your Kafka event-type topics into per-connector Apache Iggy topics, read-only.

Joins your Kafka cluster as its own consumer group ("iggy-shadow"), so the Ruby producers, the
routing app, the export pipeline and every existing consumer are untouched. For each event it
looks up which connectors of that app are configured for that event type (connectors.json, the
same shape as your integration settings) and writes it to each one's topic, keyed by user ID.
Then run: python connector.py --apps 0   (reads every connector topic in the stream) and compare.

Needs: pip install confluent-kafka apache-iggy==0.9.0
Map your records in to_event(): the kit needs the app, the user ID, the event type and the event id.
Pass client settings with -X, for example: -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512

connectors.json:
  {"connectors": [{"name": "app-0002-storage", "app_id": "<app id>", "events": ["*"]},
                  {"name": "app-0002-analytics", "app_id": "<app id>", "events": ["Purchase", "Custom Event"]}]}
"""
import argparse
import asyncio
import json
import re
import time
import zlib
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer as KafkaConsumer

from common import DEFAULT_URL, STREAM, connect, ensure_topic, msg


def to_event(topic: str, value: bytes):
    """Return (app id, user id, event type, event time ns, event id) from one of your records, or None to skip it."""
    d = json.loads(value)
    app, user = d.get("app_id"), d.get("user_id") or d.get("external_user_id")
    name = d.get("event_type") or topic                  # one topic per event type: the topic names the type
    if not (app and user):
        return None
    return str(app), str(user), name, int(d.get("ts_ns") or time.time_ns()), str(d.get("id", ""))


async def main(a):
    iggy = await connect(a.url)
    cfg = json.load(open(a.config))["connectors"]
    routes = defaultdict(list)                           # app id -> [(topic, event types or None for all)]
    for c in cfg:
        t = re.sub(r"[^A-Za-z0-9_.-]", "_", c["name"])[:200]
        await ensure_topic(iggy, STREAM, t, a.partitions)
        parts = (await iggy.get_topic(STREAM, t)).partitions_count
        routes[str(c["app_id"])].append((t, parts, None if "*" in c["events"] else set(c["events"])))
    kafka = KafkaConsumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow",
                           "auto.offset.reset": "latest", "enable.auto.commit": True,
                           **dict(kv.split("=", 1) for kv in a.X)})
    kafka.subscribe(a.kafka_topics)
    seq, stats = defaultdict(int), defaultdict(int)
    mark = time.monotonic()
    while True:
        batches = defaultdict(list)
        for rec in kafka.consume(num_messages=5000, timeout=0.005):
            if rec.error():
                continue
            ev = to_event(rec.topic(), rec.value())
            if ev is None:
                continue
            app, user, name, ns, eid = ev
            for t, parts, wanted in routes.get(app, ()):
                if wanted is None or name in wanted:
                    seq[t, user] += 1            # per-user order as read from Kafka; restart the bridge = new run
                    batches[t, zlib.crc32(user.encode()) % parts].append(
                        SendMessage(msg(ns=ns, id=eid, u=user, seq=seq[t, user], e=name)))
                    stats["copied"] += 1
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, t, Partitioning.partition_id(pid), ms)
                                   for (t, pid), ms in batches.items()))
        if time.monotonic() - mark >= 1:
            print(f"[bridge] {stats['copied']:,} events/s copied into {sum(map(len, routes.values()))} connector topics",
                  flush=True)
            stats.clear()
            mark = time.monotonic()
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--kafka-topics", nargs="+", required=True, help="your event-type topics")
    p.add_argument("--config", required=True, help="connectors.json: which event types each connector exports")
    p.add_argument("--partitions", type=int, default=1, help="partitions for a new connector topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
