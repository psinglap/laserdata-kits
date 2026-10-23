"""Shadow your card attempt records from Kafka into the Apache Iggy attempt lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): every producer, every existing consumer and every
Flink job on your Kafka clusters are untouched. For each record it publishes one attempt, keyed by business,
with a per-(host, business) sequence, the same shape api_nodes.py writes. Point detector.py at Iggy and
compare its interventions and timings with your card testing controls'.

Map your record in to_attempt(): the kit needs the business (account) ID, the amount, a card fingerprint
(never the card number), the outcome (approved, declined, challenge_failed or challenge_passed) and, if you
have them, the host and surface. The defaults read JSON with fields account_id, amount, card_fingerprint,
outcome, host and surface. Amounts are in major units (dollars), as api_nodes.py writes them.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import ATTEMPTS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg, part


def to_attempt(value: bytes, a):
    d = json.loads(value)
    out = str(d[a.outcome_field])
    return (str(d[a.business_field]), float(d[a.amount_field]) / a.amount_divisor, str(d[a.card_field]), out,
            str(d.get(a.host_field, "unknown")), str(d.get(a.surface_field, "unknown")))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, ATTEMPTS)
    producer = await iggy.producer(STREAM, ATTEMPTS, topic_partitions_count=PARTITIONS[ATTEMPTS])
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
                    b, amt, card, out, host, surf = to_attempt(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not an attempt record this mapping understands
                seq[(host, b)] += 1
                groups[part(b, PARTITIONS[ATTEMPTS])].append(
                    SendMessage(msg(s=host, b=b, surf=surf, amt=amt, card=card, out=out,
                                    ch=int(out.startswith("challenge")), seq=seq[(host, b)])))
            # One batch per partition, all sent at once, so a business's attempts stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="card-attempt-outcomes", help="the topic your attempt records are on")
    p.add_argument("--business-field", default="account_id")
    p.add_argument("--amount-field", default="amount")
    p.add_argument("--amount-divisor", type=float, default=1, help="100 if amounts are in cents")
    p.add_argument("--card-field", default="card_fingerprint")
    p.add_argument("--outcome-field", default="outcome")
    p.add_argument("--host-field", default="host")
    p.add_argument("--surface-field", default="surface")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
