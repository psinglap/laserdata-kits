"""Shadow your scooter events from Kafka into the Apache Iggy lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow") on the topic your IoT broker gateway publishes vehicle
events to: the scooters, the MQTT broker, every producer and the stream processors are untouched. For each record
it routes one event, keyed by scooter, by its age on the scooter's clock (common.route()): fresh events and
crashes to rider-events, older ones to reconnect-backlog. Point alert_service.py and ride_history.py at Iggy and
compare their alerts and timings with your production notifications.

Map your record in to_event(): the kit needs the scooter ID, the event kind, the scooter's own timestamp, and the
packet sequence if the record carries one (the bridge numbers events per scooter otherwise, which drops the
resend check). The defaults read JSON with fields vehicle_id, event, device_ts_ms, seq, charging, soc, parked,
lat and lon; set KINDS to your event names. In a shadow run only the lane is timed; the clock starts when the
bridge reads the record.
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

from common import (BACKLOG, CHARGE_DONE, CHARGE_START, CRASH, DEFAULT_URL, LANE, MOVED, PARTITIONS, STREAM,
                    ensure_topics, msg, part, route)

# Your event names -> the kit's. Anything not listed is a state update ("soc") that raises no alert.
KINDS = {"charging_started": CHARGE_START, "charging_complete": CHARGE_DONE, "theft_tow": MOVED, "crash": CRASH,
         "ride_start": "ride_start", "ride_end": "ride_end"}


def to_event(value: bytes):
    d = json.loads(value)
    return str(d["vehicle_id"]), dict(
        k=KINDS.get(d.get("event"), "soc"), vt=int(d["device_ts_ms"]) * 1_000_000, seq=d.get("seq"),
        ch=int(bool(d.get("charging", 0))), soc=int(d.get("soc", 0)), pk=int(bool(d.get("parked", 1))),
        lat=float(d.get("lat", 0)), lon=float(d.get("lon", 0)))


async def main(a):
    async def producer(topic):
        c = IggyClient.from_connection_string(a.url)      # one connection per producer
        await c.connect()
        await ensure_topics(c, topic)
        return await c.producer(STREAM, topic, topic_partitions_count=PARTITIONS[topic])

    out = {LANE: await producer(LANE), BACKLOG: await producer(BACKLOG)}
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq = defaultdict(int)
    async with out[LANE], out[BACKLOG]:
        while True:
            groups = defaultdict(list)
            now_ns = time.time_ns()
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    s, ev = to_event(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a vehicle event this mapping understands
                if ev["seq"] is None:
                    seq[s] += 1
                    ev["seq"] = seq[s]
                t = route(ev["k"], ev["vt"], now_ns)
                groups[(t, part(s, PARTITIONS[t]))].append(SendMessage(msg(s=s, **ev)))
            # One batch per partition, all sent at once, so a scooter's events stay in order.
            await asyncio.gather(*(out[t].send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for (t, p), ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="vehicle-events", help="the topic your vehicle events land on")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
