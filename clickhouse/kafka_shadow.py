"""Mirror a Kafka topic that a ClickPipe already reads into Iggy, read-only, so both pipes load the same events.

Runs as its own Kafka consumer group ("iggy-shadow"): the ClickPipe's consumer group, its offsets and your producers
are untouched. Each record becomes one otel-logs event keyed by its Kafka topic and partition (one source per
partition, so per-source order is kept), with EventNs taken from the Kafka record timestamp. iggy_pipe.py then
prints Kafka-produce-to-ClickHouse freshness for the Iggy path, next to what your ClickPipe shows for the same topic.

JSON records that carry OpenTelemetry fields (ServiceName, SeverityText, Body, TraceId) keep them; anything else is
inserted as Body. Point iggy_pipe.py at a scratch database (--database) so the shadow never writes your tables.

Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, LOGS, PARTITIONS, STATE, STREAM, ensure_topics, event, part


def fields(value: bytes) -> dict:
    try:
        d = json.loads(value)
        if isinstance(d, dict):
            return d
    except ValueError:
        pass
    return {"Body": value.decode("utf-8", "replace")}


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, LOGS, STATE)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True, "auto.commit.interval.ms": 1000}   # offsets once a second
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe(a.kafka_topic)
    P = PARTITIONS[LOGS]
    seq = defaultdict(int)
    forwarded, t0 = 0, time.monotonic()
    while True:
        batches = defaultdict(list)
        for rec in kafka.consume(num_messages=5000, timeout=0.005):
            if rec.error():
                continue
            d = fields(rec.value() or b"")
            key = f"kafka/{rec.topic()}/{rec.partition()}"
            seq[key] += 1
            ns = rec.timestamp()[1] * 1_000_000
            ra = d.get("ResourceAttributes")
            region = str(ra.get("cloud.region", "")) if isinstance(ra, dict) else ""
            batches[part(key, P)].append(SendMessage(event(
                ns, key, region, str(d.get("ServiceName", rec.topic())), f"partition-{rec.partition()}", seq[key],
                str(d.get("SeverityText", "INFO")), str(d.get("Body", ""))[:a.max_body], str(d.get("TraceId", "")))))
            forwarded += 1
        if batches:
            # One batch per partition, all sent at once; a Kafka partition's records keep their order.
            await asyncio.gather(*(iggy.send_messages(STREAM, LOGS, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        now = time.monotonic()
        if now - t0 >= 1:
            print(f"[shadow] {forwarded:,} records/s mirrored from {', '.join(a.kafka_topic)}", flush=True)
            forwarded, t0 = 0, now
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers (the ones your ClickPipe reads)")
    p.add_argument("--kafka-topic", nargs="+", default=["otel-logs"], help="the topic(s) your ClickPipe reads")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--max-body", type=int, default=16384, help="truncate Body to this many characters")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
