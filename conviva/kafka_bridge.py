"""Shadow your ad events from Kafka into the Apache Iggy lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the sensors, the gateway, every producer, the Time-State
jobs and Druid and ClickHouse are untouched. For each ad event it publishes one record, keyed by session, with the
session's event sequence, the same shape live_match.py writes; everything else on the topic is skipped. Point
ad_break_checker.py at Iggy and compare its cohort alerts and timings with your production alerts.

Map your record in to_event(): the kit needs the session ID, the event kind (break_start, attempt, impression,
response_failure, creative_media_failure, slate, break_end), the ad server, market and device. The defaults read
JSON with fields session_id, event, ad_server, market and device_type. In a shadow run only the lane is timed; the
clock starts when the bridge reads the record.
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

from common import AD_EVENTS, DEFAULT_URL, PARTITIONS, STREAM, UNFILLED, ensure_topics, msg, part

KINDS = {"break_start", "attempt", "impression", "break_end", *UNFILLED}


def to_event(value: bytes):
    d = json.loads(value)
    k = d["event"]
    if k not in KINDS:
        return None
    return str(d["session_id"]), dict(k=k, srv=str(d["ad_server"]), mkt=str(d["market"]), dev=str(d["device_type"]))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, AD_EVENTS)
    producer = await iggy.producer(STREAM, AD_EVENTS, topic_partitions_count=PARTITIONS[AD_EVENTS])
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
                    r = to_event(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a record this mapping understands
                if r is None:
                    continue                # not an ad event: heartbeats and the rest stay on Kafka only
                s, ev = r
                seq[s] += 1
                groups[part(s, PARTITIONS[AD_EVENTS])].append(SendMessage(msg(s=s, seq=seq[s], **ev)))
            # One batch per partition, all sent at once, so a session's ad events stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="session-events", help="the topic your sessions' ad events land on")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
