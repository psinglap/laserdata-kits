"""Shadow your viewed-post events from Redpanda into the Apache Iggy viewed-posts lane, read-only.

Redpanda speaks the Kafka API, so this runs as its own Kafka consumer group ("iggy-shadow"):
the apps, the Flink job that persists views, and every existing consumer are untouched. It groups
each read batch by user, adds a sequence (the read time in ns, so it only grows, even across
bridge restarts), and publishes to the lane keyed by user, with
the same partitioning as the kit. Point dedup.py at Iggy and compare.

Map your records in to_view(): the kit needs the user ID and the viewed post ID (or a list of them).
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for SASL/SCRAM on Redpanda Cloud:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-256 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STREAM, VIEWED, ensure_topics, msg, part


def to_view(value: bytes):
    d = json.loads(value)
    # Rename your fields here, for example userId -> user, postId or postIds -> posts.
    user = str(d.get("user") or d.get("userId") or d.get("user_id"))
    posts = d.get("posts") or d.get("postIds") or [d.get("postId") or d.get("post_id")]
    return user, [str(p) for p in posts if p]


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, VIEWED)
    producer = await iggy.producer(STREAM, VIEWED, topic_partitions_count=PARTITIONS[VIEWED])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            by_user = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                user, posts = to_view(rec.value())
                by_user[user] += posts
            groups = defaultdict(list)
            s = time.time_ns()                          # one flush per user per read batch, newer than any before
            for user, posts in by_user.items():
                groups[part(user, PARTITIONS[VIEWED])].append(SendMessage(msg(user=user, seq=s, posts=posts)))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Redpanda bootstrap brokers")
    p.add_argument("--kafka-topic", required=True, help="your viewed-post events topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
