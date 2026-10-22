"""Shadow your delivery attempt results from Kafka into the Apache Iggy outcomes lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the delivery workers, every producer and every existing
consumer (Delivery Overview, the discard table, alerting) are untouched. For each record it publishes one outcome,
keyed by workspace:destination, with a per-worker sequence, the same shape delivery.py writes. Point alerts.py at
Iggy and compare its ALERT and RESOLVED records and their timing with your own delivery alerts.

Map your record in to_outcome(): the kit needs the workspace, the destination (instance) ID, the HTTP status or
result, and optionally the attempt number and the worker host. The defaults read JSON with fields workspace_id,
destination_id, status, attempt and host; a status in --ok-statuses (2xx by default) counts as delivered.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example (SASL/SCRAM):
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, OUTCOMES, PARTITIONS, STREAM, ensure_topics, key, msg, part


def to_outcome(value: bytes, a):
    d = json.loads(value)
    k = key(str(d[a.workspace_field]), str(d[a.destination_field]))
    status = str(d[a.status_field])
    ok = status.startswith(tuple(a.ok_statuses.split(",")))
    return k, ok, status, int(d.get(a.attempt_field, 1)), str(d.get(a.host_field, "unknown"))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, OUTCOMES)
    producer = await iggy.producer(STREAM, OUTCOMES, topic_partitions_count=PARTITIONS[OUTCOMES])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq = defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    k, ok, status, attempt, host = to_outcome(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not a delivery result this mapping understands
                seq[(host, k)] += 1
                groups[part(k, PARTITIONS[OUTCOMES])].append(
                    SendMessage(msg(k=k, w=host, seq=seq[(host, k)], ok=ok, status=status, attempt=attempt)))
            # One batch per partition, all sent at once, so a key's outcomes stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", default="delivery-results", help="the topic your delivery attempt results are on")
    p.add_argument("--workspace-field", default="workspace_id")
    p.add_argument("--destination-field", default="destination_id")
    p.add_argument("--status-field", default="status")
    p.add_argument("--ok-statuses", default="2", help="comma-separated status prefixes that count as delivered")
    p.add_argument("--attempt-field", default="attempt")
    p.add_argument("--host-field", default="host")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
