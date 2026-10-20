"""Shadow your provider call events from MSK into the Apache Iggy outcomes lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the call workers, every producer and every existing
consumer (monitoring, analytics, call logs) are untouched. For each record it publishes one outcome, keyed by
stage:provider:region, with a per-worker sequence, the same shape call_workers.py writes. Point health.py at
Iggy and compare its DEGRADED and OK verdicts and their timing with your own failover and alerting.

Map your record in to_outcome(): the kit needs the stage (stt, llm or tts), the provider, the region, whether
the request succeeded, and optionally time to first byte and the worker host. The defaults read JSON with fields
stage, provider, region, ok, ttfb_ms and host. Run one bridge per regional cluster, inside that region.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example (MSK with SASL/SCRAM):
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
    k = key(str(d[a.stage_field]), str(d[a.provider_field]), str(d.get(a.region_field, a.region)))
    return k, bool(d[a.ok_field]), d.get(a.ttfb_field), str(d.get(a.host_field, "unknown"))


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
                    k, ok, ttfb, host = to_outcome(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not a provider event this mapping understands
                seq[(host, k)] += 1
                groups[part(k, PARTITIONS[OUTCOMES])].append(
                    SendMessage(msg(k=k, w=host, seq=seq[(host, k)], ok=ok, probe=False, ttfb=ttfb)))
            # One batch per partition, all sent at once, so a key's outcomes stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your regional MSK bootstrap brokers")
    p.add_argument("--kafka-topic", default="provider-events", help="the topic your provider call events are on")
    p.add_argument("--region", default="us", help="region to use when records don't carry one")
    p.add_argument("--stage-field", default="stage")
    p.add_argument("--provider-field", default="provider")
    p.add_argument("--region-field", default="region")
    p.add_argument("--ok-field", default="ok")
    p.add_argument("--ttfb-field", default="ttfb_ms")
    p.add_argument("--host-field", default="host")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
