"""Shadow an existing Confluent Cloud order-state topic into Apache Iggy, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so producers, the Flink jobs and every
existing consumer are untouched. Point fpt.py at Iggy and compare.

Map your records in to_msg(): the kit needs r (restaurant id), seq (per restaurant), ev, and the
resulting kitchen state: cooking, counter, running (up to five order ids) and done (last five
preparation times, minutes). If your events carry only the change, keep that state here per
restaurant and send the result.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for a Confluent Cloud API key:
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=<api-key> -X sasl.password=<api-secret>
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, KITCHEN, KITCHEN_PARTITIONS, STREAM, part


def to_msg(value: bytes) -> tuple[str, bytes]:
    d = json.loads(value)
    d.setdefault("ns", time.time_ns())
    return d["r"], json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, a.iggy_topic, topic_partitions_count=KITCHEN_PARTITIONS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            batches = defaultdict(list)          # one batch per partition, same restaurant -> partition function
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if not rec.error():
                    r, body = to_msg(rec.value())
                    batches[part(r, KITCHEN_PARTITIONS)].append(SendMessage(body))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in batches.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Confluent Cloud bootstrap server")
    p.add_argument("--kafka-topic", required=True, help="the order-state topic restaurants' events land on")
    p.add_argument("--iggy-topic", default=KITCHEN)
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
