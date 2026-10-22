"""Shadow your API request records from Kafka into the Apache Iggy request lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): every producer, every existing consumer and every
pipeline on your Kafka clusters are untouched. For each record it publishes one request, keyed by API key,
with a per-(router, key) sequence, the same shape routers.py writes. Point enforcement.py at Iggy and
compare its disables and timings with your enforcement's.

Map your record in to_request(): the kit needs the API key ID (never the secret), the project, the source
network (ASN, or a /24 if that is what you log) and optionally the router or host and the cost. The defaults
read JSON with fields api_key_id, project_id, asn, host and cost_usd.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, REQUESTS, STREAM, ensure_topics, msg, part


def to_request(value: bytes, a):
    d = json.loads(value)
    return (str(d[a.key_field]), str(d.get(a.project_field, "")), str(d[a.network_field]),
            str(d.get(a.host_field, "unknown")), float(d.get(a.cost_field, 0.0)))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, REQUESTS)
    producer = await iggy.producer(STREAM, REQUESTS, topic_partitions_count=PARTITIONS[REQUESTS])
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
                    k, proj, net, host, usd = to_request(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not a request record this mapping understands
                seq[(host, k)] += 1
                groups[part(k, PARTITIONS[REQUESTS])].append(
                    SendMessage(msg(s=host, k=k, p=proj, net=net, seq=seq[(host, k)], usd=usd)))
            # One batch per partition, all sent at once, so a key's requests stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="api-request-events", help="the topic your request records are on")
    p.add_argument("--key-field", default="api_key_id")
    p.add_argument("--project-field", default="project_id")
    p.add_argument("--network-field", default="asn")
    p.add_argument("--host-field", default="host")
    p.add_argument("--cost-field", default="cost_usd")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
