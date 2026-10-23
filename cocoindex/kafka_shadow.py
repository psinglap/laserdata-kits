"""Mirror a keyed Kafka topic that a CocoIndex app already indexes (through `cocoindex.connectors.kafka`) into Apache
Iggy, read-only, so the same flow can run on Iggy beside it.

Runs as its own Kafka consumer group ("iggy-shadow"): your app's consumer group, its offsets and your producers are
untouched. Each record becomes one message in the shape index_app.py reads: the Kafka key as `thread_id`, the record
value as `body`, the Kafka record timestamp as `ns`, and a per-key `seq`. A tombstone (null value) becomes
`{"_deleted": key}`, the deletion shape CocoIndex's own Iggy examples use. Then:

    python index_app.py --topic shadow-events --table shadow_context
    python context_check.py --topic shadow-events --table shadow_context

prints Kafka-produce-to-LanceDB freshness for the Iggy path, next to the Kafka path you run today.

Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, SHADOW, STREAM, ensure_topic


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topic(iggy, SHADOW)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": a.offset_reset,
            "enable.auto.commit": True, "auto.commit.interval.ms": 1000}   # offsets once a second
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe(a.kafka_topic)
    seq = defaultdict(int)
    forwarded, t0 = 0, time.monotonic()
    while True:
        batch = []
        for rec in kafka.consume(num_messages=5000, timeout=0.005):
            if rec.error() or rec.key() is None:
                continue
            key = rec.key().decode("utf-8", "replace")
            seq[key] += 1
            ns = rec.timestamp()[1] * 1_000_000
            if rec.value() is None:
                d = {"_deleted": key, "seq": seq[key], "ns": ns}
            else:
                d = {"thread_id": key, "seq": seq[key], "ns": ns,
                     "body": rec.value().decode("utf-8", "replace")[:a.max_body]}
            batch.append(SendMessage(json.dumps(d, separators=(",", ":")).encode()))
        if batch:
            # One partition, in Kafka order per key: CocoIndex's Iggy source reads one partition per source today.
            await iggy.send_messages(STREAM, SHADOW, Partitioning.partition_id(0), batch)
            forwarded += len(batch)
        now = time.monotonic()
        if now - t0 >= 1:
            print(f"[shadow] {forwarded:,} records/s mirrored from {', '.join(a.kafka_topic)}", flush=True)
            forwarded, t0 = 0, now
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers (the ones your CocoIndex app reads)")
    p.add_argument("--kafka-topic", nargs="+", required=True, help="the keyed topic(s) your CocoIndex app reads")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--offset-reset", default="earliest", help="earliest: mirror the topic's current state first")
    p.add_argument("--max-body", type=int, default=16384, help="truncate the value to this many characters")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
