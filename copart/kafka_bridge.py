"""Shadow an existing live-auction feed into Apache Iggy, read-only, from a Kafka topic.

Reads as its own consumer group ("iggy-shadow") and never commits offsets, so the auction services that produce
the feed, and every existing consumer, are untouched. Works with any endpoint that speaks the Kafka protocol
(pass SASL or TLS settings with -X).

Point bid_edge.py at Iggy and compare. Map your records in to_state(): the kit needs k (the sale lane), lot, st
("open" or "sold"), high (the high bid), erv (estimated retail value, or 0), dl (when the countdown runs out, in
ns), bid (the id of the bid that made this state, or ""), and seq (per lane, increasing). Copart's data job posts
name Pub/Sub, Kinesis and Kafka; which one carries VB3's bid events, if any, is not public. If it is Pub/Sub or
Kinesis, swap the consumer below for that client; the rest stays the same.

Needs: pip install apache-iggy==0.9.0 confluent-kafka
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, LOT_STATE, STREAM, connect, ensure_topics, part_of


def to_state(body: bytes) -> tuple[str, bytes]:
    d = json.loads(body)                    # map your field names here
    d.setdefault("bid", "")
    d.setdefault("ns", time.time_ns())      # when the bridge saw it, if the record has no creation time in ns
    return d["k"], json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    from confluent_kafka import Consumer

    iggy = await connect(a.url)
    await ensure_topics(iggy, LOT_STATE)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": False}    # leave no committed offsets behind
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    while True:
        recs = [r for r in kafka.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            batches = defaultdict(list)     # same partition function as the kit: one lane, one partition
            for r in recs:
                k, payload = to_state(r.value())
                batches[part_of(k)].append(SendMessage(payload))
            await asyncio.gather(*(iggy.send_messages(STREAM, LOT_STATE, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="bootstrap servers of the Kafka cluster")
    p.add_argument("--topic", default="vb3-lot-state", help="the topic that carries live lot state")
    p.add_argument("-X", action="append", default=[], metavar="key=value",
                   help="extra client setting, e.g. -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
