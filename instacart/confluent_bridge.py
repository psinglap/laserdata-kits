"""Shadow your session events from Confluent Cloud into the session sequence lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow") on the topic your storefront events land on (or the
sub-topic your Flink router writes them to): the apps, the event ingestion service, every producer, every
Flink job and every existing consumer are untouched. For each product view, item page visit or cart add it
publishes one action, keyed by user, with a sequence that only grows (the read time in ns, plus one per
record), with the same partitioning as the kit. Point retrieval.py at Iggy and compare its sequences with the
session your ads retrieval reads today.

Instacart's event schema is not public. Map your record in to_action(): the defaults look for common field
names (user_id, event or event_name, product_id or sid, retailer_type, session_id). In a shadow run only the
lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Confluent Cloud settings go in with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=<api key> -X sasl.password=<api secret>
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import ACTIONS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg, part

KIND = {"product_view": "product_view", "item_page_visit": "item_page_visit", "item_detail_view": "item_page_visit",
        "add_to_cart": "add_to_cart", "cart_add": "add_to_cart"}


def to_action(value: bytes):
    d = json.loads(value)
    kind = KIND.get(str(d.get("event") or d.get("event_name") or ""))
    if kind is None:
        return None                         # not one of the session events the sequence keeps
    user = str(d.get("user_id") or d["userId"])
    sess = str(d.get("session_id") or d.get("sessionId") or user)
    token = str(d.get("sid") or d.get("product_id") or d["item_id"])
    rt = str(d.get("retailer_type") or d.get("retailer_id") or "")
    return user, sess, kind, token, rt


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
                    act = to_action(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a record this mapping understands
                if act is None:
                    continue
                user, sess, kind, token, rt = act
                seq += 1
                groups[part(user, PARTITIONS[ACTIONS])].append(
                    SendMessage(msg(user=user, sess=sess, seq=seq, kind=kind, sid=token, rt=rt)))
            # One batch per partition, all sent at once, so a shopper's actions stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Confluent Cloud bootstrap server")
    p.add_argument("--kafka-topic", default="storefront-events", help="the topic your session events land on")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
