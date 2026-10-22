"""Shadow your authorization outcomes from Kafka into the Apache Iggy account spend lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the authorization switch, every producer and every
existing consumer are untouched. It keeps the same per-account, per-category spend as switch.py and
publishes each approval's or reversal's resulting spend to the lane, keyed by account. Point
feature_store.py at Iggy and compare with the feature store you run today.

Map your records in to_outcome(): the kit needs the account, the card, the MCC or category, the amount in
paise, whether it was approved or reversed, and a reference. Records keep the same account partitioning as
the kit, so each account's spend stays in order. In a shadow run only the lane is timed; the clock starts
when the bridge reads the record. Spend starts at zero when the bridge starts, so compare from the next
cycle boundary, or seed it from your store.
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

from common import DEFAULT_URL, PARTITIONS, SPEND, STREAM, Spend, ensure_topics, msg, part

MCC_GROUPS = {"5411": "grocery", "5541": "fuel", "5542": "fuel", "5812": "dining", "5814": "dining",
              "4511": "travel", "7011": "travel", "5999": "online", "6011": "cash"}   # extend with your MCC map


def to_outcome(value: bytes):
    d = json.loads(value)
    # Rename your fields here, for example accountId -> account, cardId -> card, txnAmount -> amount.
    account = str(d.get("account") or d.get("accountId") or d.get("account_id"))
    card = str(d.get("card") or d.get("cardId") or "")
    mcc = str(d.get("mcc") or "")
    category = d.get("category") or MCC_GROUPS.get(mcc, "online")
    amount = int(d.get("amount") or d.get("amountInPaise") or 0)
    ref = str(d.get("ref") or d.get("transactionId") or "")
    status = str(d.get("status", "APPROVED")).upper()
    return account, card, category, amount, ref, status


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, SPEND)
    producer = await iggy.producer(STREAM, SPEND, topic_partitions_count=PARTITIONS[SPEND])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    sp = Spend()
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                account, card, category, amount, ref, status = to_outcome(rec.value())
                if status in ("APPROVED", "AUTHORIZED"):
                    delta, kind = amount, "APPROVED"
                elif status in ("REVERSED", "REVERSAL", "RELEASED"):
                    delta, kind = -amount, "REVERSED"
                else:
                    continue                    # declines do not move spend
                v = sp.apply(account, category, delta)
                groups[part(account, PARTITIONS[SPEND])].append(SendMessage(msg(kind=kind, ref=ref, card=card, cat=category, **v)))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    p.add_argument("--kafka-topic", required=True, help="your authorization outcome topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
