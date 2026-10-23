"""Shadow your business events from Kafka into the Apache Iggy broadcast push lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the event producers, the Event Subscription Platform and
every existing consumer are untouched. For each record it does what event_processor.py does: a unicast
notification goes to unicast-notifications keyed by subscription; a broadcast is split into window tasks on
broadcast-windows at partition window % 16. Point push_workers.py at Iggy with its HTTP call left simulated (or
aimed at a test endpoint) and compare its broadcast completion times with your own.

Map your record in to_event(): the kit needs the notification topic and either the subscription ID (unicast) or
the number of subscriptions on the topic (broadcast). The defaults read JSON with fields topic, subscriptionId and
broadcast, and take the broadcast subscription count from --broadcast-subs.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SSL -X ssl.ca.location=...
"""
import argparse
import asyncio
import json
import time
import zlib
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STREAM, TASKS, UNICAST, ensure_topics, msg, part, windows


def to_event(value: bytes, a):
    d = json.loads(value)
    if d.get(a.broadcast_field):
        return str(d[a.topic_field]), None
    return str(d[a.topic_field]), str(d[a.sub_field])


async def main(a):
    async def own_client():                 # one connection per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), UNICAST, TASKS)
    uni = await (await own_client()).producer(STREAM, UNICAST, topic_partitions_count=PARTITIONS[UNICAST])
    tasks = await (await own_client()).producer(STREAM, TASKS, topic_partitions_count=PARTITIONS[TASKS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe(a.kafka_topic.split(","))
    seq = defaultdict(int)
    n = 0
    n_win = windows(a.broadcast_subs, a.window_size)
    async with uni, tasks:
        while True:
            ug, tg = defaultdict(list), defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    topic, s = to_event(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not an event this mapping understands
                if s is not None:
                    seq[s] += 1
                    i = zlib.crc32(s.encode()) % 1_000_000   # stands in for the endpoint in the simulated push
                    ug[part(s, PARTITIONS[UNICAST])].append(SendMessage(msg(s=s, i=i, seq=seq[s], t=topic)))
                else:
                    n += 1
                    e, ens = f"shadow-{n:08d}", time.time_ns()
                    for w in range(n_win):
                        tg[w % PARTITIONS[TASKS]].append(SendMessage(msg(
                            e=e, w=w, subs=a.broadcast_subs, size=a.window_size, t=topic, ns=ens)))
            # One batch per partition, all sent at once, so a subscription's notifications stay in order.
            await asyncio.gather(*(uni.send_with_partitioning(ms, Partitioning.partition_id(p)) for p, ms in ug.items()),
                                 *(tasks.send_with_partitioning(ms, Partitioning.partition_id(p)) for p, ms in tg.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", default="business-events", help="topic(s) your raw business events are on")
    p.add_argument("--topic-field", default="topic")
    p.add_argument("--sub-field", default="subscriptionId")
    p.add_argument("--broadcast-field", default="broadcast")
    p.add_argument("--broadcast-subs", type=int, default=20_000, help="subscriptions on a broadcast topic")
    p.add_argument("--window-size", type=int, default=200)
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
