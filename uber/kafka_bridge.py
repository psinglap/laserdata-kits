"""Shadow existing demand and supply topics from Kafka into the Apache Iggy surge lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so producers, the surge pipeline and every existing
consumer are untouched; if this process stops, only the shadow stops. Each record becomes one event on
surge-events, on the partition of its hexagon, with the same shape marketplace.py writes. Run surge_window.py
with your window and compare its distinct counts with what your own pipeline computed for the same windows.

Map your records with the --*-field options (JSON values assumed; change to_event() for other encodings):
the kit needs the hexagon (an H3 cell ID works as is), the product, the rider or driver ID, and the event time
in ms. In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 ...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, EVENTS, PARTITIONS, STREAM, ensure_topics, msg, part


def to_event(rec, a, kind: str):
    d = json.loads(rec.value())
    h = str(d[a.hex_field])
    t = int(d.get(a.time_field) or rec.timestamp()[1] or time.time() * 1000)
    return h, msg(k=kind, h=h, p=str(d.get(a.product_field, "all")), u=str(d[a.id_field]), t=t)


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, EVENTS)
    producer = await iggy.producer(STREAM, EVENTS, topic_partitions_count=PARTITIONS[EVENTS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kinds = {a.demand_topic: "d", a.supply_topic: "s"}
    kafka.subscribe(list(kinds))
    sent, t0 = 0, time.monotonic()
    async with producer:
        while True:
            # One batch per partition, the same partition function as marketplace.py, so each hexagon stays in order.
            batches = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    h, payload = to_event(rec, a, kinds[rec.topic()])
                except (KeyError, ValueError, TypeError):
                    continue                    # a record without the fields the lane needs
                batches[part(h, PARTITIONS[EVENTS])].append(SendMessage(payload))
                sent += 1
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in batches.items()))
            if time.monotonic() - t0 >= 1:
                print(f"[bridge] {sent:,} events/s from {', '.join(kinds)}", flush=True)
                sent, t0 = 0, time.monotonic()
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--demand-topic", required=True, help="the topic carrying rider demand events")
    p.add_argument("--supply-topic", required=True, help="the topic carrying driver supply events")
    p.add_argument("--hex-field", default="hexagon")
    p.add_argument("--product-field", default="product")
    p.add_argument("--id-field", default="id", help="rider ID on demand, driver ID on supply")
    p.add_argument("--time-field", default="ts_ms", help="event time in ms (the record timestamp if missing)")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
