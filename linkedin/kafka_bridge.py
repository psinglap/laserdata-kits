"""Shadow a tracking topic into the rt-signals lane, read-only, for a trial on real traffic shape and rate.

Today the latency-sensitive signals go from Tracking API straight to real-time processing, and Kafka carries the
near-real-time and offline copy. This bridge reads one or more of those Kafka tracking topics in its own consumer
group ("iggy-shadow"), so Tracking API, every existing consumer and their offsets are untouched: a new consumer
group gets its own copy of every record and moves no one else's position. (If those topics run on Northguard
behind Xinfra, point the bridge at whatever Kafka-protocol endpoint your Xinfra setup exposes, or swap the
Consumer below for your Xinfra client.)

Each record becomes one signal on the lane, keyed by a pseudonymized member ID (hashed here, before it enters
the lane), with a per-member sequence number in the order the bridge reads them, the event type and the platform.
Map your record in to_signal(): tracking records are Avro, so decode them there with your schema registry client;
the default reads JSON with a dotted path to the member field. In a shadow run the clock starts when the bridge
reads the record, so only the lane is timed.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SSL -X ssl.ca.location=...
"""
import argparse
import asyncio
import hashlib
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, RT, STREAM, ensure_topics, msg, part


def dig(d: dict, path: str):
    for k in path.split("."):
        d = d.get(k) if isinstance(d, dict) else None
    return d


def to_signal(topic: str, value: bytes, a) -> tuple[str, str, str] | None:
    """Return (member key, event type, platform) or None to skip. Decode Avro here for real tracking records."""
    try:
        d = json.loads(value)
    except ValueError:
        return None
    mid = dig(d, a.member_field)
    if mid is None:
        return None
    pm = "pm-" + hashlib.blake2b(f"{a.salt}:{mid}".encode(), digest_size=6).hexdigest()   # pseudonymize first
    return pm, str(dig(d, a.type_field) or topic), str(dig(d, a.platform_field) or "unknown")


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, RT)
    producer = await iggy.producer(STREAM, RT, topic_partitions_count=PARTITIONS[RT])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe(a.topic)
    run, seq, n = f"bridge-{time.time_ns():x}", defaultdict(int), 0
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                s = to_signal(rec.topic(), rec.value(), a)
                if s is None:
                    continue                # not a record this mapping understands
                pm, t, plat = s
                seq[pm] += 1
                groups[part(pm, PARTITIONS[RT])].append(SendMessage(msg(run=run, m=pm, seq=seq[pm], t=t, p=plat)))
                n += 1
            # One batch per partition, all sent at once, so a member's signals stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--topic", action="append", required=True, help="tracking topic to shadow (repeatable)")
    p.add_argument("--member-field", default="header.memberId", help="dotted path to the member ID")
    p.add_argument("--type-field", default="eventType", help="dotted path to the event type (default: topic name)")
    p.add_argument("--platform-field", default="requestHeader.platform")
    p.add_argument("--salt", required=True, help="secret salt for pseudonymizing member IDs")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
