"""Shadow your authentication events from Kafka into the Apache Iggy authentication lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): every producer, every existing consumer and every
pipeline on your Kafka clusters are untouched. For each record it publishes one authentication, keyed by account,
with a per-(DC, account) sequence, the same shape dc_sensors.py writes. Point detection.py at Iggy and compare its
enforcements and timings with your identity detections'.

Map your record in to_auth(): the kit needs the account (with the customer ID if accounts of many customers share
the topic), the source and destination hosts, and optionally the DC and protocol. The defaults read JSON with
fields cid, account, source_host, target_host, dc and protocol.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import AUTH, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg, part


def to_auth(value: bytes, a):
    d = json.loads(value)
    acct = f"{d.get(a.cid_field, '')}/{d[a.account_field]}"
    return (acct, str(d[a.src_field]), str(d[a.dst_field]), str(d.get(a.dc_field, "unknown")),
            str(d.get(a.proto_field, "")))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, AUTH)
    producer = await iggy.producer(STREAM, AUTH, topic_partitions_count=PARTITIONS[AUTH])
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
                    acct, src, dst, dc, proto = to_auth(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not an authentication record this mapping understands
                seq[(dc, acct)] += 1
                groups[part(acct, PARTITIONS[AUTH])].append(
                    SendMessage(msg(s=dc, a=acct, src=src, dst=dst, proto=proto, seq=seq[(dc, acct)])))
            # One batch per partition, all sent at once, so an account's authentications stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="identity-auth-events", help="the topic your authentication events are on")
    p.add_argument("--cid-field", default="cid")
    p.add_argument("--account-field", default="account")
    p.add_argument("--src-field", default="source_host")
    p.add_argument("--dst-field", default="target_host")
    p.add_argument("--dc-field", default="dc")
    p.add_argument("--proto-field", default="protocol")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
