"""Shadow an existing otlp_logs Kafka topic into Apache Iggy, read-only.

If your collectors already write to Kafka with the Kafka exporter (otlp_proto encoding, the default),
this joins that topic as its own consumer group ("iggy-shadow"), so the exporter, your SigNoz
collectors reading the topic and every other consumer are untouched. Each Kafka record becomes one
batch on the otlp_logs topic in Iggy, in the shape writer.py reads. Point writer.py at a test
ClickHouse and compare freshness with your current path.

Needs: pip install confluent-kafka opentelemetry-proto apache-iggy==0.9.0
Pass client settings with -X, for example SASL/SCRAM:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer
from opentelemetry.proto.logs.v1.logs_pb2 import LogsData

from common import DEFAULT_URL, LOGS, STREAM, msg, partition


def to_batches(value: bytes, tenant: str, seq: dict) -> list:
    """One OTLP LogsData payload -> one kit batch per service. Adapt the tenant and service mapping here."""
    data, out = LogsData(), []
    data.ParseFromString(value)
    for rl in data.resource_logs:
        svc = next((kv.value.string_value for kv in rl.resource.attributes if kv.key == "service.name"), "")
        recs = [[lr.time_unix_nano or lr.observed_time_unix_nano, lr.severity_text, lr.body.string_value]
                for sl in rl.scope_logs for lr in sl.log_records]
        if recs:
            seq[tenant] += 1
            out.append(msg(t=tenant, seq=seq[tenant], svc=svc, recs=recs))
    return out


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, LOGS, topic_partitions_count=a.partitions)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq, t0, n = defaultdict(int), time.monotonic(), 0
    async with producer:
        while True:
            by_part = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                tenant = (rec.key() or a.tenant.encode()).decode(errors="replace")
                for b in to_batches(rec.value(), tenant, seq):
                    by_part[partition(tenant, a.partitions)].append(SendMessage(b))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in by_part.items()))
            n += sum(len(ms) for ms in by_part.values())
            if time.monotonic() - t0 >= 5:
                print(f"[bridge] copied {n:,} batches in the last 5 s", flush=True)
                t0, n = time.monotonic(), 0
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="otlp_logs")
    p.add_argument("--tenant", default="tenant-shadow", help="tenant name when records have no key")
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
