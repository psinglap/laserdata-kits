"""Shadow the payment token notifications you publish to Kafka today into Apache Iggy, read-only.

Reads as its own consumer group ("iggy-shadow") and never commits offsets, so the payments platform's producers
and every existing consumer are untouched. Works with any Kafka API endpoint (pass SASL or TLS settings with -X).

Point instalments.py at Iggy and compare. Map your records in to_change(): the kit needs k (customer), seq (per
customer, increasing), token, status ("active" or "revoked"). If your records are Avro, decode them here with your
schema registry client. Check your own HMAC here too; the bridge re-signs with the lane's key, because the kit's
subscribers verify every record.

Needs: pip install apache-iggy==0.9.0 confluent-kafka
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, STREAM, TOKENS, connect, ensure_topics, mask_record, part_of, sign


def to_change(body: bytes) -> tuple[str, bytes]:
    d = json.loads(body)                    # map your field names here
    d.setdefault("ns", time.time_ns())      # when the bridge saw it, if the record has no creation time in ns
    mask_record(d)                          # the guard runs on the copy too
    return d["k"], sign(json.dumps(d, separators=(",", ":")).encode())


async def main(a):
    from confluent_kafka import Consumer

    iggy = await connect(a.url)
    await ensure_topics(iggy, TOKENS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": False}    # leave no committed offsets behind
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    while True:
        recs = [r for r in kafka.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            batches = defaultdict(list)     # same partition function as the kit: one customer, one partition
            for r in recs:
                k, payload = to_change(r.value())
                batches[part_of(k)].append(SendMessage(payload))
            await asyncio.gather(*(iggy.send_messages(STREAM, TOKENS, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers")
    p.add_argument("--topic", default="payment-token-changes", help="the token notification topic subscribers read today")
    p.add_argument("-X", action="append", default=[], metavar="key=value",
                   help="extra Kafka client setting, e.g. -X security.protocol=SASL_SSL")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
