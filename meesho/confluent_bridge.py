"""Shadow the Confluent Cloud topic your services push order notifications to, into Apache Iggy, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the services that push to Meecom,
Meecom itself and the Confluent cluster are untouched. Point meecom_sender.py at Iggy and compare.

Map your records in to_msg(): the kit needs k (order ID), state and ch (channel); seq is
added here per order if your records do not carry one (Kafka keeps one key's records in
order within a partition, so a per-key counter is safe).
Needs: pip install confluent-kafka apache-iggy==0.9.0
Confluent Cloud settings go in with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN -X sasl.username=<api-key> -X sasl.password=<api-secret>
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, ORDER_UPDATES, PARTITIONS, STREAM, part

seq = defaultdict(int)


def to_msg(key: bytes | None, value: bytes, ts_ms: int | None) -> tuple[str, bytes]:
    d = json.loads(value)
    k = d.get("k") or d.get("order_id") or d.get("orderId") or (key or b"none").decode()
    seq[k] = max(seq[k] + 1, d.get("seq", 0))
    out = {"k": k, "seq": seq[k], "state": d.get("state") or d.get("event") or d.get("template"),
           "ch": d.get("ch") or d.get("channel") or "push",
           "ns": ts_ms * 1_000_000 if ts_ms else time.time_ns()}     # Kafka CreateTime, so latency includes the hop
    return k, json.dumps(out, separators=(",", ":")).encode()


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, a.iggy_topic, topic_partitions_count=PARTITIONS[ORDER_UPDATES])
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
                batches[part(k, PARTITIONS[ORDER_UPDATES])].append(SendMessage(body))
            # one batch per partition, same partition function as order_services.py, sent at once
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in batches.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Confluent Cloud bootstrap server")
    p.add_argument("--kafka-topic", required=True, help="the topic your services push order notifications to today")
    p.add_argument("--iggy-topic", default=ORDER_UPDATES)
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
