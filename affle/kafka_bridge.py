"""Shadow your clicks and install postbacks from Kafka into the Apache Iggy click-install lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the click trackers, the postback receivers, every
producer and every existing consumer (attribution, mFaaS, reporting) are untouched. For each record it publishes
one click or install, keyed by device, with a per-host sequence, the same shape attribution_nodes.py writes.
Point ctit_scorer.py at Iggy and compare its blocks and their timing with the blocks mFaaS makes today.

Map your records in to_event(): the kit needs the event type (click or install), the device ID, the publisher
(or sub-publisher) for clicks, and the event time. The defaults read JSON with fields type, device_id, publisher,
ts_ms and host. Run one bridge per Kafka cluster, next to it. In a shadow run only the lane is timed; the clock
starts when the bridge reads the record, and CTIT is taken from your event times.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example (SASL/SCRAM):
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, EVENTS, PARTITIONS, STREAM, ensure_topics, msg, part


def to_event(value: bytes, a):
    d = json.loads(value)
    kind = str(d[a.type_field]).lower()
    e = "c" if kind in a.click_types else "i" if kind in a.install_types else None
    if e is None:
        raise ValueError(kind)
    ns = int(float(d[a.ts_field]) * 1e6) if a.ts_field in d else time.time_ns()
    return e, str(d[a.device_field]), str(d.get(a.publisher_field, "")), ns, str(d.get(a.host_field, "unknown"))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, EVENTS)
    producer = await iggy.producer(STREAM, EVENTS, topic_partitions_count=PARTITIONS[EVENTS])
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
                    e, dev, pub, ns, host = to_event(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not a click or install this mapping understands
                seq[(host, dev)] += 1
                f = {"pub": pub} if e == "c" else {}
                # The lane's latency clock starts here; the event time travels as "t" for CTIT.
                groups[part(dev, PARTITIONS[EVENTS])].append(
                    SendMessage(msg(e=e, d=dev, w=host, seq=seq[(host, dev)], t=ns, **f)))
            # One batch per partition, all sent at once, so a device's events stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", action="append", default=None,
                   help="a topic with clicks or install postbacks (repeat for several; default: clicks, installs)")
    p.add_argument("--type-field", default="type")
    p.add_argument("--click-types", default="click", type=lambda s: set(s.split(",")))
    p.add_argument("--install-types", default="install", type=lambda s: set(s.split(",")))
    p.add_argument("--device-field", default="device_id")
    p.add_argument("--publisher-field", default="publisher")
    p.add_argument("--ts-field", default="ts_ms")
    p.add_argument("--host-field", default="host")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    args = p.parse_args()
    args.kafka_topic = args.kafka_topic or ["clicks", "installs"]
    asyncio.run(main(args))
