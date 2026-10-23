"""Shadow an existing listing-change feed into Apache Iggy, read-only, from a Confluent Kafka topic.

Reads as its own consumer group ("iggy-shadow") and never commits offsets, so the seller integrations and the
pricing service that produce the feed, and every existing consumer, are untouched. Works with Confluent Cloud or
any endpoint that speaks the Kafka protocol (pass SASL or TLS settings with -X).

Point zone_picker.py at Iggy and compare. Map your records in to_change(): the kit needs k (listing), z
("event:zone", the venue map zone the listing sits in), seq (per listing, increasing), qty (tickets left), price
(per ticket) and zq (Zone Deals tickets checkout has processed on the listing; send 0 if you have no such
counter). If listing changes don't reach a Kafka topic today (the public sources don't say), the evaluation
starts with a tap where seller API updates and price changes are written.

Needs: pip install apache-iggy==0.9.0 confluent-kafka
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, LISTINGS, STREAM, connect, ensure_topics, part_of


def to_change(body: bytes) -> tuple[str, bytes]:
    d = json.loads(body)                    # map your field names here
    d.setdefault("zq", 0)
    d.setdefault("ns", time.time_ns())      # when the bridge saw it, if the record has no creation time in ns
    return d["z"], json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    from confluent_kafka import Consumer

    iggy = await connect(a.url)
    await ensure_topics(iggy, LISTINGS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": False}    # leave no committed offsets behind
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    while True:
        recs = [r for r in kafka.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            batches = defaultdict(list)     # same partition function as the kit: one event's zone, one partition
            for r in recs:
                z, payload = to_change(r.value())
                batches[part_of(z)].append(SendMessage(payload))
            await asyncio.gather(*(iggy.send_messages(STREAM, LISTINGS, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="bootstrap servers of the Confluent cluster")
    p.add_argument("--topic", default="listing-changes", help="the topic that carries listing changes")
    p.add_argument("-X", action="append", default=[], metavar="key=value",
                   help="extra client setting, e.g. -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
