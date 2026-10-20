"""Shadow an existing position-update topic from your Strimzi Kafka into Apache Iggy, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the changefeed, the Holdings service
and every existing consumer are untouched. Point holdings.py at Iggy and compare.

Map your records in to_msg(): the kit needs k ("user:instrument"), u (user ID), seq, qty and avg.
A CockroachDB changefeed wraps the row in {"after": {...}}; the mapping below unwraps it.
Records keep the same user partitioning as the kit, so each user's updates stay in order.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for SCRAM inside the cluster:
  -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, POSITIONS, STREAM, ensure_topics, msg, part


def to_msg(value: bytes):
    d = json.loads(value)
    d = d.get("after") or d                 # changefeed envelope, or a plain record
    # Rename your columns here, for example user_id -> u, net_quantity -> qty, average_price -> avg.
    u = str(d.get("u") or d.get("user_id"))
    k = d.get("k") or f"{u}:{d.get('instrument') or d.get('symbol')}"
    out = dict(k=k, u=u, seq=int(d.get("seq") or d.get("version") or time.time_ns()),
               qty=d.get("qty", d.get("net_quantity")), avg=d.get("avg", d.get("average_price")))
    return u, msg(**out)                    # ns = when the bridge read it, so the lane is timed from here


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, POSITIONS)
    producer = await iggy.producer(STREAM, POSITIONS, topic_partitions_count=PARTITIONS[POSITIONS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if not rec.error():
                    u, m = to_msg(rec.value())
                    groups[part(u, PARTITIONS[POSITIONS])].append(SendMessage(m))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap, for example the Strimzi bootstrap service")
    p.add_argument("--kafka-topic", required=True, help="your position-update topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
