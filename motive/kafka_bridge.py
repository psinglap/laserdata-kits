"""Shadow your cargo sensor events from Kafka into the Apache Iggy lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the gateways, the ingestion services, every producer
and the Flink jobs (geofencing, trips, alerts, asset location) are untouched. For each record it publishes one
event, keyed by asset, with the asset's event sequence and whole current state, the same shape fleet.py writes.
Point alert_rules.py at Iggy and compare its alerts and timings with your production alerts.

Map your record in to_event(): the kit needs the asset ID, whether it is a door change, door state and when it
last changed, cargo temperature and set point, and position. The defaults read JSON with fields asset_id,
event_type ("door" or anything else), door_open, door_changed_ns, temperature_c, set_point_c, lat and lon.
Your approved geofences go in common.sites(). In a shadow run only the lane is timed; the clock starts when the
bridge reads the record.
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

from common import DEFAULT_URL, PARTITIONS, READINGS, STREAM, ensure_topics, msg, part


def to_event(value: bytes):
    d = json.loads(value)
    return str(d["asset_id"]), dict(
        k="door" if d.get("event_type") == "door" else "reading", door=int(bool(d.get("door_open", 0))),
        door_ns=int(d.get("door_changed_ns", 0)), temp=float(d["temperature_c"]), sp=float(d["set_point_c"]),
        lat=float(d["lat"]), lon=float(d["lon"]))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, READINGS)
    producer = await iggy.producer(STREAM, READINGS, topic_partitions_count=PARTITIONS[READINGS])
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
                    k, ev = to_event(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a cargo sensor record this mapping understands
                seq[k] += 1
                groups[part(k, PARTITIONS[READINGS])].append(SendMessage(msg(a=k, seq=seq[k], **ev)))
            # One batch per partition, all sent at once, so an asset's events stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="asset-sensor-events", help="the topic your cargo sensor events land on")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
