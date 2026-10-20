"""Shadow your completed-payment events from your Kafka into the Apache Iggy velocity lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the IngestionService, every producer and
every existing consumer are untouched. It keeps the same rolling per-payer counts as payments.py and
publishes each payment's resulting counts to the lane, keyed by payer. Point risk.py at Iggy and compare.

Map your records in to_payment(): the kit needs the payer, the amount in paise, the payee and a
transaction ID. Records keep the same payer partitioning as the kit, so each payer's counts stay in order.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record. The counts
start empty and are complete after the first 10 minutes (the longest window).
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

from common import DEFAULT_URL, PARTITIONS, STREAM, VELOCITY, Velocity, ensure_topics, msg, part


def to_payment(value: bytes):
    d = json.loads(value)
    # Rename your fields here, for example senderId -> payer, amountInPaise -> amount, receiverId -> payee.
    payer = str(d.get("payer") or d.get("senderId") or d.get("payer_id"))
    payee = str(d.get("payee") or d.get("receiverId") or d.get("payee_id"))
    amount = int(d.get("amount") or d.get("amountInPaise") or 0)
    txn = str(d.get("txn") or d.get("transactionId") or "")
    ok = str(d.get("status", "COMPLETED")).upper() in ("COMPLETED", "SUCCESS")
    return payer, amount, payee, txn, ok


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, VELOCITY)
    producer = await iggy.producer(STREAM, VELOCITY, topic_partitions_count=PARTITIONS[VELOCITY])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    vel = Velocity()
    async with producer:
        while True:
            groups = defaultdict(list)
            now_s = time.monotonic()
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                payer, amount, payee, txn, ok = to_payment(rec.value())
                if not ok:
                    continue                    # only completed payments move the counts
                v = vel.add(payer, amount, payee, now_s)
                groups[part(payer, PARTITIONS[VELOCITY])].append(SendMessage(msg(txn=txn, **v)))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in groups.items()))
            vel.sweep(now_s)
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of the Kafka read cluster")
    p.add_argument("--kafka-topic", required=True, help="your completed-payment events topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
