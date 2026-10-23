"""Shadow the bandit decisions Expedia Group already publishes into Apache Iggy, read-only, from a Kafka topic.

The Inference Engine publishes a "bandit proposed action" event for every decision. This bridge reads that topic
as its own consumer group ("iggy-shadow") and never commits offsets, so the Samplers that publish it and every
existing consumer (the feedback gatherer, the reward collector, offline evaluation) are untouched. Works with any
endpoint that speaks the Kafka protocol (pass SASL or TLS settings with -X).

Point sampler.py at Iggy and compare. Map your records in to_decision(): the kit needs k (session or visitor id),
b (bandit, or bandit and slot, e.g. the property for a hero image), arm (the arm shown), by (the Sampler pod) and
ns (when it was picked, in nanoseconds).

Needs: pip install apache-iggy==0.9.0 confluent-kafka
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DECISIONS, DEFAULT_URL, STREAM, connect, ensure_topics, part_of


def to_decision(body: bytes) -> tuple[str, bytes]:
    d = json.loads(body)                    # map your field names here
    d.setdefault("by", "kafka")
    d.setdefault("ns", time.time_ns())      # when the bridge saw it, if the record has no pick time in ns
    return d["k"], json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    from confluent_kafka import Consumer

    iggy = await connect(a.url)
    await ensure_topics(iggy, DECISIONS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": False}    # leave no committed offsets behind
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    while True:
        recs = [r for r in kafka.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            batches = defaultdict(list)     # same partition function as the kit: one session, one partition
            for r in recs:
                k, payload = to_decision(r.value())
                batches[part_of(k)].append(SendMessage(payload))
            await asyncio.gather(*(iggy.send_messages(STREAM, DECISIONS, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="bootstrap servers of the Kafka-protocol endpoint")
    p.add_argument("--topic", default="bandit-proposed-action", help="the topic that carries the bandit decisions")
    p.add_argument("-X", action="append", default=[], metavar="key=value",
                   help="extra client setting, e.g. -X security.protocol=SASL_SSL")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
