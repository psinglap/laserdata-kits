"""Shadow one building's carton events into the site lane, read-only, from a Kafka topic you already have.

Runs as its own consumer group ("iggy-shadow") on the topic where your fulfillment events already land, so the
WMS, the control system and every existing consumer are untouched: a new consumer group gets its own copy of
every message and moves no one else's offset.

Each record is expected to be JSON with a carton (or LPN) field, an event-type field and, optionally, an event ID.
--steps maps your event types onto the lane's six steps (PICKED, PACKED, LABELED, SORTED, LOADED,
SHIP_CONFIRMED); other event types are skipped. The bridge adds the lane fields, keyed by carton: a per-carton
sequence and the resulting step. An event ID it has already bridged (Kafka delivers at least once too) is
skipped, using a bounded window of recent IDs (--dedupe-window). In a shadow run the clock starts when the bridge
reads the record. If your events are not on Kafka, the same mapping works from any feed you can read.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SASL_SSL -X sasl.mechanisms=AWS_MSK_IAM ...
"""
import argparse
import asyncio
import json
import time
from collections import OrderedDict, defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, EVENTS, FC, PARTITIONS, STEPS, connect, ensure_topics, msg, part


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy, EVENTS)
    producer = await iggy.producer(FC, EVENTS, topic_partitions_count=PARTITIONS[EVENTS])
    steps = dict(kv.split("=", 1) for kv in a.steps.split(",")) if a.steps else {s: s for s in STEPS}
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    seq = defaultdict(int)
    recent = OrderedDict()                  # event IDs already bridged, oldest first
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    r = json.loads(rec.value())
                    carton, kind = str(r[a.carton_field]), str(r[a.type_field])
                except (KeyError, ValueError, TypeError):
                    continue                # not an event this mapping understands
                step = steps.get(kind)
                if step not in STEPS:
                    continue
                eid = r.get(a.id_field)
                if eid is not None:
                    if eid in recent:
                        continue            # Kafka delivered it again: already bridged
                    recent[eid] = None
                    if len(recent) > a.dedupe_window:
                        recent.popitem(last=False)
                seq[carton] = max(seq[carton] + 1, STEPS.index(step) + 1)
                b = msg(site=a.site, carton=carton, order=r.get(a.order_field), seq=seq[carton], step=step,
                        dev="kafka-shadow", door=r.get(a.door_field), ns=time.time_ns())
                groups[part(carton, PARTITIONS[EVENTS])].append(SendMessage(b))
            # One batch per partition, all sent at once, so a carton's events stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="your Kafka brokers, host:port")
    ap.add_argument("--topic", required=True, help="the topic one building's fulfillment events land on")
    ap.add_argument("--site", default="AVP2")
    ap.add_argument("--carton-field", default="carton")
    ap.add_argument("--type-field", default="type")
    ap.add_argument("--id-field", default="eventId")
    ap.add_argument("--order-field", default="order")
    ap.add_argument("--door-field", default="door")
    ap.add_argument("--steps", default="", help="map your event types: PICK=PICKED,PACK=PACKED,... (default: same names)")
    ap.add_argument("--dedupe-window", type=int, default=2_000_000, help="recent event IDs remembered")
    ap.add_argument("--url", default=DEFAULT_URL, help="the building's broker")
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
