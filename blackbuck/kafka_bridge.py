"""Shadow your formatted GPS pings from Kafka into the Apache Iggy lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the devices, the load balancer, the ping formatter,
every producer and every downstream service (timeline, alarms, warehouse) are untouched. For each record that
carries a fuel reading it publishes one ping, keyed by device, with the device's ping sequence, in the shape
trucks.py writes. Point fuel_rules.py at Iggy and compare its drain and refuel events with your production ones.

Map your record in to_ping(): the kit needs the device ID, fuel level in litres, ignition, speed and position.
The defaults read JSON with fields device_id, fuel_litres, ignition (0/1 or true/false), speed_kmph, lat and lon;
records without a fuel reading are skipped. In a shadow run only the lane is timed; the clock starts when the
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

from common import DEFAULT_URL, PARTITIONS, PINGS, STREAM, ensure_topics, msg, part


def to_ping(value: bytes):
    d = json.loads(value)
    if d.get("fuel_litres") is None:
        return None                         # a ping from a device without a fuel sensor
    return str(d["device_id"]), dict(
        fuel=float(d["fuel_litres"]), ign=int(bool(d.get("ignition", 0))), spd=int(float(d.get("speed_kmph", 0))),
        lat=float(d["lat"]), lon=float(d["lon"]))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, PINGS)
    producer = await iggy.producer(STREAM, PINGS, topic_partitions_count=PARTITIONS[PINGS])
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
                    r = to_ping(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a ping this mapping understands
                if r is None:
                    continue
                k, ping = r
                seq[k] += 1
                groups[part(k, PARTITIONS[PINGS])].append(SendMessage(msg(d=k, seq=seq[k], **ping)))
            # One batch per partition, all sent at once, so a device's pings stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="gps-pings", help="the topic your formatted device pings land on")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
