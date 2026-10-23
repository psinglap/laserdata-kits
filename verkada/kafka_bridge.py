"""Shadow your camera partition events from Kafka into the Apache Iggy lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): cameras, access control, Command, every producer, the
response workflows and the monitoring console are untouched. For each record it publishes one event, keyed by
alarm partition, with that partition's event sequence, the same shape sites.py writes. Point alarm_state.py at
Iggy and compare its countdowns and raised alarms with what your alarm service did for the same events.

Map your record in to_event(): the kit needs the alarm partition ID, the event kind (a camera detection, an arm,
a disarm, a panic press), the event's time, and the partition's entry delay. The defaults read JSON with fields
partition_id, event_type ("camera_trigger", "arm", "disarm", "panic"), event_time_ns and entry_delay_s.
If detections, access events and arm/disarm commands sit on different topics, pass --kafka-topic once per topic.
Order across topics is the order this bridge reads them, so run it close to your brokers. The entry delay
counts from each event's own time, so in a shadow run event-to-state also includes your pipeline's time before
Kafka.
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

from common import ARM, DEFAULT_URL, DETECT, DISARM, EVENTS, PANIC, PARTITIONS, STREAM, ensure_topics, msg, part

KINDS = {"camera_trigger": DETECT, "arm": ARM, "disarm": DISARM, "panic": PANIC}


def to_event(value: bytes):
    d = json.loads(value)
    return str(d["partition_id"]), dict(k=KINDS[d["event_type"]], ns=int(d.get("event_time_ns") or time.time_ns()),
                                        delay_s=float(d.get("entry_delay_s", 30)))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, EVENTS)
    producer = await iggy.producer(STREAM, EVENTS, topic_partitions_count=PARTITIONS[EVENTS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe(a.kafka_topic or ["alarm-partition-events"])
    seq = defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    k, ev = to_event(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not an alarm partition record this mapping understands
                seq[k] += 1
                groups[part(k, PARTITIONS[EVENTS])].append(SendMessage(msg(ap=k, seq=seq[k], **ev)))
            # One batch per partition, all sent at once, so a partition's events stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", action="append", default=[], help="a topic your alarm partition events land on")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
