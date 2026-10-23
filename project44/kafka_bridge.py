"""Shadow your normalized truck positions and shipment changes from Kafka into the Apache Iggy lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the carrier integrations, the ingestion pipeline, the
tracking systems, every producer and every consumer group you run on your Kafka clusters today are untouched. For
each record it publishes one event, keyed by vehicle, with that vehicle's event sequence, the same shape fleet.py
writes. Point share_gate.py at Iggy and compare what it shares, and when, with what your tracking systems share
today.

Map your records in to_event(): the kit needs the vehicle (license plate or asset ID) and one of three kinds:
  * pos: latitude, longitude and the time the position was taken;
  * assign: a shipment put on this vehicle, with the customer, pick-up and delivery coordinates and the time the
    tracking window opens;
  * stop: the shipment is no longer on this vehicle (sharing stopped, cancelled or moved to another tractor).
The defaults read JSON with fields vehicle_id, event ("pos", "assign" or "stop"), lat, lon, shipment_id,
customer_id, pickup [lat, lon], delivery [lat, lon] and window_start_ms. If positions and shipment changes live on
different topics, pass --kafka-topic twice; a vehicle's events keep their order inside this bridge only when they
arrive in order, so read them from the same consumer. Compressed topics (Snappy and the rest) are decompressed by
the client. In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for a Strimzi listener with SCRAM:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, ensure_topics, msg, part


def to_event(value: bytes):
    d = json.loads(value)
    v, k = str(d["vehicle_id"]), d["event"]
    if k == "pos":
        return v, dict(k="pos", lat=float(d["lat"]), lon=float(d["lon"]))
    if k == "assign":
        return v, dict(k="assign", sh=str(d["shipment_id"]), c=str(d["customer_id"]),
                       pu=[float(x) for x in d["pickup"]], dr=[float(x) for x in d["delivery"]],
                       win=int(d["window_start_ms"]) * 1_000_000)
    if k == "stop":
        return v, dict(k="stop", sh=str(d["shipment_id"]), why=str(d.get("reason", "stopped")))
    raise ValueError(k)


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, LANE)
    producer = await iggy.producer(STREAM, LANE, topic_partitions_count=PARTITIONS[LANE])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe(a.kafka_topic)
    seq = defaultdict(lambda: time.time_ns() // 1000)   # rising across restarts, as fleet.py does
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    v, ev = to_event(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a record this mapping understands
                seq[v] += 1
                groups[part(v, PARTITIONS[LANE])].append(SendMessage(msg(v=v, seq=seq[v], **ev)))
            # One batch per partition, all sent at once, so a vehicle's events stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers (e.g. the Strimzi bootstrap service)")
    p.add_argument("--kafka-topic", action="append", default=None,
                   help="the topic your normalized positions (and shipment changes) land on; repeat for several")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    args = p.parse_args()
    args.kafka_topic = args.kafka_topic or ["vehicle-positions"]
    asyncio.run(main(args))
