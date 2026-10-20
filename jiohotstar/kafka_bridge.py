"""Shadow your minimized heartbeat topic from Kafka into Apache Iggy, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the clients, the heartbeat splitter and
every existing consumer are untouched. Point qos.py at Iggy and compare.

Map your records in decode() and to_msg(): the kit needs k (cohort|cdn, for example
"airtel:KA|cdn-b"), seq, sess (session ID), st ("playing", "buffering" or "failed") and rtt (ms).
Your HeartbeatEvent is protobuf: decode it with your generated class in decode(); the default
reads JSON so the bridge runs as is. Only cohort, CDN, session ID, playback state and RTT cross
the bridge: no user, device or content traits are copied into the lane.
Records keep the kit's key partitioning, so each pair's heartbeats stay in order.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for SASL:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, HEARTBEATS, PARTITIONS, STREAM, ensure_topics, msg, part

seq = defaultdict(int)


def decode(value: bytes) -> dict:
    # For protobuf: e = HeartbeatEvent(); e.ParseFromString(value); return {...the fields below...}
    return json.loads(value)


def to_msg(value: bytes):
    d = decode(value)
    # Rename your fields here: cohort as your routing uses it (ASN-State, or ASN-Country-State-City-UserType),
    # the CDN serving the session, and the playback state of this heartbeat.
    cohort = d.get("cohort") or f"{d.get('asn')}:{d.get('state')}"
    k = d.get("k") or f"{cohort}|{d.get('cdn')}"
    seq[k] = max(seq[k] + 1, time.time_ns())  # rises across bridge restarts, so the check never drops new records
    st = d.get("st") or ("failed" if d.get("playback_failed") else "buffering" if d.get("rebuffering") else "playing")
    # ns = when the bridge read it, so the lane is timed from here
    return k, msg(k=k, seq=seq[k], sess=d.get("session_id") or d.get("sess"), st=st, rtt=d.get("rtt") or 0, deg=0)


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, HEARTBEATS)
    producer = await iggy.producer(STREAM, HEARTBEATS, topic_partitions_count=PARTITIONS[HEARTBEATS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if not rec.error():
                    k, m = to_msg(rec.value())
                    groups[part(k, PARTITIONS[HEARTBEATS])].append(SendMessage(m))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", required=True, help="your minimized heartbeat topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
