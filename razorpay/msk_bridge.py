"""Shadow your payment-status topic from Amazon MSK into Apache Iggy, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the Payment Status Job, the downtime
detector and every existing consumer are untouched. Point downtime.py at Iggy and compare.

Map your records in to_msg(): the kit needs k (cohort, for example "upi:psp:okhdfcbank"), seq,
pay (payment ID) and status ("captured" or anything else for a failure). The mapping below builds
the cohort from method plus instrument, the way the Downtime API names them.
Records keep the kit's cohort partitioning, so each cohort's payments stay in order.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for SASL/SCRAM on MSK (port 9096):
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STATUS, STREAM, ensure_topics, msg, part

seq = defaultdict(int)


def cohort(d: dict) -> str:
    m = d.get("method")
    if m == "upi":
        return f"upi:psp:{(d.get('vpa') or '').split('@')[-1] or d.get('psp')}"
    if m == "card":
        return f"card:issuer:{d.get('issuer')}:{d.get('card_type')}"
    if m == "netbanking":
        return f"netbanking:bank:{d.get('bank')}"
    return f"{m}:other"


def to_msg(value: bytes):
    d = json.loads(value)
    d = d.get("payment") or d                # an event envelope, or a plain record
    # Rename your fields here. Only cohort, payment ID and final status cross the bridge:
    # no VPA, card number or customer detail is copied into the lane.
    k = d.get("k") or cohort(d)
    seq[k] = max(seq[k] + 1, time.time_ns())  # rises across bridge restarts, so the detector never drops new records
    status = "captured" if d.get("status") in ("captured", "authorized", "success") else "failed"
    # ns = when the bridge read it, so the lane is timed from here
    return k, msg(k=k, seq=seq[k], pay=d.get("id") or d.get("pay"), status=status, deg=0)


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, STATUS)
    producer = await iggy.producer(STREAM, STATUS, topic_partitions_count=PARTITIONS[STATUS])
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
                    groups[part(k, PARTITIONS[STATUS])].append(SendMessage(m))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="MSK bootstrap brokers")
    p.add_argument("--kafka-topic", required=True, help="your payment-status topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
