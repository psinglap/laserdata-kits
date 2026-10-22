"""Shadow your payment status events from Kafka into the Apache Iggy lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow") on the topic your payment status (or merchant
notification) events land on: the payment switch, every producer, the notification services and the IoT path to
the Soundboxes are untouched. For each record it routes one status event, keyed by Soundbox, by how long ago the
customer paid (common.route()): fresh confirmations to payment-confirmations, older ones to late-confirmations.
Point announcer.py and passbook.py at Iggy and compare their announcements and timings with your production
Soundbox notifications.

Map your record in to_event(): the kit needs the device (or merchant terminal) ID, the status, the amount, the
time the customer paid and the transaction ID, plus the device's event sequence and today's total and count if
the record carries them (the bridge numbers events and keeps totals per device otherwise, which is fine for a
shadow run). The defaults read JSON with fields device_id, status, method, amount_paise, txn_time_ms, txn_id,
seq, day_total_paise, day_count and lang. In a shadow run only the lane is timed; the clock starts when the
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

from common import (CARD, DEFAULT_URL, FAILED, LANE, LATE, PARTITIONS, PENDING, STREAM, UPI, ensure_topics, msg,
                    part, route)

# Your status and method names -> the kit's kinds. Anything not listed is treated as pending (no announcement).
SUCCESS = {"upi": UPI, "card": CARD}
STATUS = {"SUCCESS": "success", "PENDING": PENDING, "FAILURE": FAILED, "FAILED": FAILED}


def to_event(value: bytes):
    d = json.loads(value)
    st = STATUS.get(str(d.get("status", "")).upper(), PENDING)
    k = SUCCESS.get(str(d.get("method", "upi")).lower(), UPI) if st == "success" else st
    return str(d["device_id"]), dict(
        k=k, amt=int(d.get("amount_paise", 0)) if st == "success" else 0, vt=int(d["txn_time_ms"]) * 1_000_000,
        txn=str(d.get("txn_id", "")), seq=d.get("seq"), tot=d.get("day_total_paise"), cnt=d.get("day_count"),
        lang=d.get("lang", "hi"))


async def main(a):
    async def producer(topic):
        c = IggyClient.from_connection_string(a.url)      # one connection per producer
        await c.connect()
        await ensure_topics(c, topic)
        return await c.producer(STREAM, topic, topic_partitions_count=PARTITIONS[topic])

    out = {LANE: await producer(LANE), LATE: await producer(LATE)}
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq, tot, cnt = defaultdict(int), defaultdict(int), defaultdict(int)
    async with out[LANE], out[LATE]:
        while True:
            groups = defaultdict(list)
            now_ns = time.time_ns()
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    s, ev = to_event(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a status event this mapping understands
                if ev["seq"] is None:
                    seq[s] += 1
                    ev["seq"] = seq[s]
                if ev["tot"] is None:
                    tot[s] += ev["amt"]
                    cnt[s] += ev["amt"] > 0
                    ev["tot"], ev["cnt"] = tot[s], cnt[s]
                t = route(ev["k"], ev["vt"], now_ns)
                groups[(t, part(s, PARTITIONS[t]))].append(SendMessage(msg(d=s, **ev)))
            # One batch per partition, all sent at once, so a device's events stay in order.
            await asyncio.gather(*(out[t].send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for (t, p), ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="payment-status", help="the topic your payment status events land on")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
