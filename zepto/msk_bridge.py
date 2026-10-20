"""Shadow an existing MSK inventory or pricing topic into Apache Iggy, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so producers, the Flink jobs and
every existing consumer are untouched. Point revalidate.py at Iggy and compare.

Map your records in to_msg(): the kit needs k ("hub:sku"), seq, and level or price.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for SASL/SCRAM on MSK:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, STREAM


def to_msg(value: bytes) -> bytes:
    d = json.loads(value)
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, a.iggy_topic, topic_partitions_count=16)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            by_key = {}
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if not rec.error():
                    by_key.setdefault(rec.key() or b"none", []).append(SendMessage(to_msg(rec.value())))
            for k, msgs in by_key.items():
                await producer.send_with_partitioning(msgs, Partitioning.messages_key(k))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="MSK bootstrap brokers")
    p.add_argument("--kafka-topic", default="inventory")
    p.add_argument("--iggy-topic", default="inventory")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
