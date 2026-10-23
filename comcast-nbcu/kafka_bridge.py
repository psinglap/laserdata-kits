"""Shadow your playback session events from Kafka (Amazon MSK or your own) into the Apache Iggy stream-limit
lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow") on the topic your players or playback service write
session events to (session start, heartbeat, stop), so every producer and every existing consumer is
untouched. It writes each event keyed by account, with a sequence per account, the same shape
stream_limit.py writes. Start stream_limit.py --read-only to read the lane, and compare its active streams
per account with your concurrency service.

Map your record in to_session(): the kit needs the account, the device, the session ID and the event type.
The defaults read JSON with fields account_id, device_id, session_id and event_type, with the types
start, heartbeat and stop. In a shadow run only the lane is timed: the clock starts when the bridge reads the
record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example (MSK with IAM auth needs its own callback; TLS shown):
  -X security.protocol=SSL -X ssl.ca.location=...
"""
import argparse
import asyncio
import json
import os
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, SESSIONS, STREAM, connect, ensure_topics, msg, part

KINDS = {"start": "start", "heartbeat": "hb", "stop": "stop"}


def to_session(value: bytes, a):
    d = json.loads(value)
    kind = KINDS.get(str(d.get(a.type_field, "")).lower())
    if kind is None:
        return None
    return str(d[a.account_field]), str(d[a.device_field]), str(d[a.session_field]), kind


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy, SESSIONS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq = defaultdict(int)
    me = f"bridge.{os.getpid()}"         # a restarted bridge is a new writer, so its sequences start again
    P = PARTITIONS[SESSIONS]
    while True:
        batches = defaultdict(list)
        for rec in kafka.consume(num_messages=1000, timeout=0.001):
            if rec.error():
                continue
            try:
                ev = to_session(rec.value(), a)
            except (KeyError, ValueError):
                continue                    # not a record this mapping understands
            if ev is None:
                continue                    # not a session start, heartbeat or stop
            acct, dev, sid, kind = ev
            seq[acct] += 1
            batches[part(acct, P)].append(SendMessage(msg(s=me, a=acct, d=dev, sid=sid, k=kind, seq=seq[acct])))
        # One batch per partition, all sent at once, so an account's events stay in order.
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, SESSIONS, Partitioning.partition_id(q), ms)
                                   for q, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="a broker of your Kafka or MSK cluster")
    ap.add_argument("--kafka-topic", default="playback-session-events", help="your playback session events topic")
    ap.add_argument("--account-field", default="account_id")
    ap.add_argument("--device-field", default="device_id")
    ap.add_argument("--session-field", default="session_id")
    ap.add_argument("--type-field", default="event_type")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
