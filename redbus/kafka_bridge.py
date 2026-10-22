"""Shadow your Kafka GPS topic into the boarding-point lane on Apache Iggy, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the GPS vendor integrations, the real-time GPS scanner,
every producer and every consumer you run today (assignment validator, GPS mapper, boarding-point detection,
notification dispatcher, booking extension) are untouched. Each record is re-published keyed by SVC_DOJ, with the
same partition function as the kit, so a journey's packets stay in order. Point tracker.py at Iggy and compare its
ENTRY, HALT and EXIT events with the ones your boarding-point detection produces.

Map your records in to_packet(). redBus's record schema is not public, so the field names are guesses
(svc_doj / service_no + doj, seq, lat / lng, ts). Without a sequence in the record, the device timestamp is used.
tracker.py needs each journey's assignment (its boarding points in route order and passengers per point) before
its packets: pass a second topic with --assign-topic and map it in to_assign(), or seed it from your own data.
In a shadow run the clock starts when the bridge reads the record, so only the lane is timed.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SSL
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, GPS, PARTITIONS, STREAM, ensure_topics, msg, part


def key_of(d: dict) -> str:
    return str(d.get("svc_doj") or d.get("SVC_DOJ") or f"{d['service_no']}_{d['doj']}")


def to_packet(value: bytes) -> tuple[str, dict]:
    d = json.loads(value)
    ts_ms = int(d.get("ts") or d.get("timestamp") or time.time() * 1000)
    return key_of(d), {"t": "gps", "seq": int(d.get("seq") or ts_ms), "lat": float(d["lat"]),
                       "lon": float(d.get("lng") or d["lon"])}


def to_assign(value: bytes) -> tuple[str, dict]:
    # One record per validated assignment: boarding points in route order, with passengers boarding at each.
    d = json.loads(value)
    bps = [[bp.get("name", ""), float(bp["lat"]), float(bp.get("lng") or bp["lon"]), int(bp.get("passengers", 0))]
           for bp in d["boarding_points"]]
    return key_of(d), {"t": "assign", "bps": bps}


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)       # one connection for the one producer
    await iggy.connect()
    await ensure_topics(iggy, GPS)
    producer = await iggy.producer(STREAM, GPS, topic_partitions_count=PARTITIONS[GPS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    topics = [a.kafka_topic] + ([a.assign_topic] if a.assign_topic else [])
    kafka.subscribe(topics)
    print(f"[bridge] reading {', '.join(topics)} as group iggy-shadow, from the latest offset, read-only", flush=True)
    async with producer:
        while True:
            groups = defaultdict(list)
            recs = await asyncio.to_thread(kafka.consume, 1000, 0.05)
            for rec in recs:
                if rec.error():
                    continue
                try:
                    k, body = (to_assign if rec.topic() == a.assign_topic else to_packet)(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a record this mapping understands
                groups[part(k, PARTITIONS[GPS])].append(SendMessage(msg(k=k, **body)))
            # One batch per partition, all sent at once, so a journey's packets stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", required=True, help="the topic the GPS scanner writes latest packets to")
    p.add_argument("--assign-topic", help="a topic of validated journey assignments, if you have one")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
