"""Shadow an existing Kafka topic of captain pings into Apache Iggy, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the captain app's ingestion,
the services that read the topic today and the Kafka cluster are untouched.
Point match.py at Iggy and compare.

Map your records in to_msg(): the kit needs k (captain ID), lat, lon and st ("free" or
"busy"), plus ride if the ping carries one; seq is added here per captain if your records do
not carry one (Kafka keeps one key's records in order within a partition, so a per-key
counter is safe).
Needs: pip install confluent-kafka apache-iggy==0.9.0
Cluster security settings go in with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username=<user> -X sasl.password=<password>
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PINGS, STREAM, part

seq = defaultdict(int)


def to_msg(key: bytes | None, value: bytes, ts_ms: int | None) -> tuple[str, bytes]:
    d = json.loads(value)
    k = d.get("k") or d.get("captain_id") or d.get("rider_id") or (key or b"none").decode()
    seq[k] = max(seq[k] + 1, d.get("seq", 0))
    st = d.get("st") or ("busy" if d.get("ride") or d.get("order_id") else "free")
    out = {"k": k, "seq": seq[k], "lat": d["lat"], "lon": d["lon"], "st": st, "ride": d.get("ride") or d.get("order_id"),
           "ns": ts_ms * 1_000_000 if ts_ms else time.time_ns()}   # Kafka CreateTime, so latency includes the hop
    return k, json.dumps(out, separators=(",", ":")).encode()


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, a.iggy_topic, topic_partitions_count=a.partitions)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            batches = defaultdict(list)
            for rec in kafka.consume(num_messages=2000, timeout=0.001):
                if rec.error():
                    continue
                ts_type, ts_ms = rec.timestamp()
                k, body = to_msg(rec.key(), rec.value(), ts_ms if ts_type else None)
                batches[part(k, a.partitions)].append(SendMessage(body))
            # one batch per partition, same partition function as captains.py, sent at once
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in batches.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers")
    p.add_argument("--kafka-topic", required=True, help="the topic your captain pings land on today")
    p.add_argument("--iggy-topic", default=PINGS)
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
