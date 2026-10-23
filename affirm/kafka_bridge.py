"""Shadow your user-level aggregate events from Kafka into the Apache Iggy customer aggregate lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the lending system of record, the feature platform's
consumer and every other consumer are untouched. Each record becomes one aggregate on the lane, keyed by
customer with the same partition function as the kit, so one customer's aggregates stay in order.
Point features.py at Iggy and compare its stored values and write-path outcomes with your online store.

Map your records in to_aggregate(): the kit needs the customer, the system of record's counter and the
schema version; every other field is carried as is. In a shadow run only the lane is timed (the clock
starts when the bridge reads the record); there are no checkout reads unless you add them.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for SASL/SCRAM:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
If your topic is Avro or Protobuf behind Schema Registry, decode it in to_aggregate() (Apache Iggy 0.9 has
no schema registry of its own; the kit carries JSON).
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import AGGREGATES, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg, part


def to_aggregate(value: bytes):
    d = json.loads(value)
    # Rename your fields here, for example user_id -> u, version -> v. Tokenize customer IDs as your data
    # rules require; the lane only needs a stable key per customer.
    u = str(d.pop("u", None) or d.pop("user_id", None) or d.pop("userId", None) or "")
    v = d.pop("v", None) or d.pop("version", None) or d.pop("counter", None)
    schema = d.pop("schema", None) or d.pop("schema_version", None) or 1
    return u, v, schema, d


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, AGGREGATES)
    producer = await iggy.producer(STREAM, AGGREGATES, topic_partitions_count=PARTITIONS[AGGREGATES])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                u, v, schema, rest = to_aggregate(rec.value())
                if not u or v is None:
                    continue
                rest.pop("ns", None)
                groups[part(u, PARTITIONS[AGGREGATES])].append(SendMessage(msg(u=u, v=int(v), schema=int(schema), **rest)))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of the Kafka cluster that carries the aggregate events")
    p.add_argument("--kafka-topic", required=True, help="the user-level aggregate topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
