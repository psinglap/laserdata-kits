"""Shadow your shopper events from Kafka into the session context lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow") on the topic your user-context events land on (or
the clickstream topic they are built from): the app, the event collector, every producer and every existing
consumer, Storm topology or Spark job are untouched. For each record it publishes one action, keyed by user,
with a sequence that only grows (the read time in ns, plus one per record), with the same partitioning as
the kit. Point context.py at Iggy and compare its context with the one your type-ahead reads today.

Map your record in to_action(). The defaults read the event contract from Myntra's contextual type-ahead
post: {"userid", "annotations": [{"field", "value", "weight"}], "timestamp", "source"}, with an optional
session ID. In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import ACTIONS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg, part


def to_action(value: bytes):
    d = json.loads(value)
    user = str(d.get("userid") or d.get("uidx") or d["user_id"])
    sess = str(d.get("sessionId") or d.get("session_id") or user)
    ann = [(str(x["field"]), str(x["value"]), float(x.get("weight", 1))) for x in d.get("annotations", [])]
    kind = str(d.get("source") or d.get("event") or "event")
    return user, sess, kind, ann


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)       # one connection for the one producer
    await iggy.connect()
    await ensure_topics(iggy, ACTIONS)
    producer = await iggy.producer(STREAM, ACTIONS, topic_partitions_count=PARTITIONS[ACTIONS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    print(f"[bridge] reading {a.kafka_topic} as group iggy-shadow, from the latest offset, read-only", flush=True)
    async with producer:
        while True:
            groups = defaultdict(list)
            recs = await asyncio.to_thread(kafka.consume, 1000, 0.05)
            seq = time.time_ns()            # newer than any sequence before, even across bridge restarts
            for rec in recs:
                if rec.error():
                    continue
                try:
                    user, sess, kind, ann = to_action(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a shopper event this mapping understands
                if not ann:
                    continue
                seq += 1
                groups[part(user, PARTITIONS[ACTIONS])].append(
                    SendMessage(msg(user=user, sess=sess, seq=seq, kind=kind, ann=ann)))
            # One batch per partition, all sent at once, so a shopper's actions stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="user-context-events", help="the topic your shopper events land on")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
