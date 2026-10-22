"""Shadow your dock scans from Amazon MSK into the Apache Iggy lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the handheld apps, the scan ingestion services, every
producer and every consumer group you run on MSK today are untouched. For each record it publishes one event,
keyed by trip, with the trip's event sequence, the same shape docks.py writes. Point dock_check.py at Iggy and
compare its misload alerts and timings with what your dock systems flag today.

Map your records in to_event(): the kit needs the trip ID and one of three kinds:
  * plan: the trip's current destinations (the whole list);
  * load: the bag or shipment ID, its destination and the handheld or loader that scanned it;
  * seal: the trailer was closed.
The defaults read JSON with fields trip_id, event ("plan", "load" or "seal"), destinations, bag_id, destination and
scanner_id. If plans and scans live on different topics, pass --kafka-topic twice; a trip's events keep their order
inside this bridge only when they arrive in order, so read both from the same consumer. In a shadow run only the
lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example (IAM auth needs the aws-msk-iam-sasl-signer package and a callback):
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, DOCK, PARTITIONS, STREAM, ensure_topics, msg, part


def to_event(value: bytes):
    d = json.loads(value)
    t, k = str(d["trip_id"]), d["event"]
    if k == "plan":
        return t, dict(k="plan", dests=[str(x) for x in d["destinations"]], rev=int(d.get("revision", 1)))
    if k == "load":
        return t, dict(k="load", bag=str(d["bag_id"]), dest=str(d["destination"]), ldr=str(d["scanner_id"]),
                       n=int(d.get("loaded", 0)))
    if k == "seal":
        return t, dict(k="seal", n=int(d.get("loaded", 0)))
    raise ValueError(k)


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, DOCK)
    producer = await iggy.producer(STREAM, DOCK, topic_partitions_count=PARTITIONS[DOCK])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe(a.kafka_topic)
    seq = defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    t, ev = to_event(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a dock record this mapping understands
                seq[t] += 1
                groups[part(t, PARTITIONS[DOCK])].append(SendMessage(msg(t=t, seq=seq[t], **ev)))
            # One batch per partition, all sent at once, so a trip's events stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your MSK bootstrap brokers")
    p.add_argument("--kafka-topic", action="append", default=None,
                   help="the topic your dock scans (and trip plans) land on; repeat for several")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    args = p.parse_args()
    args.kafka_topic = args.kafka_topic or ["dock-scans"]
    asyncio.run(main(args))
