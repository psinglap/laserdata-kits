"""Shadow an existing server-state feed into Apache Iggy, read-only, from a Kafka-protocol topic.

Reads as its own consumer group ("iggy-shadow") and never commits offsets, so the engine instances that publish
the feed and every existing consumer, including the matchmaking index, are untouched. Works with any endpoint
that speaks the Kafka protocol (pass SASL or TLS settings with -X).

Point matchmaker.py at Iggy and compare. Map your records in to_state(): the kit needs k (server), seq (per
server, increasing), place, dc, cap, n (players), st ("open", "closing" or "closed") and seen (placements the
server has processed; send 0 if you have no such counter).

Needs: pip install apache-iggy==0.9.0 confluent-kafka
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, STATE, STREAM, connect, ensure_topics, part_of


def to_state(body: bytes) -> tuple[str, bytes]:
    d = json.loads(body)                    # map your field names here
    d.setdefault("seen", 0)
    d.setdefault("ns", time.time_ns())      # when the bridge saw it, if the record has no creation time in ns
    return d["k"], json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    from confluent_kafka import Consumer

    iggy = await connect(a.url)
    await ensure_topics(iggy, STATE)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": False}    # leave no committed offsets behind
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    while True:
        recs = [r for r in kafka.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            batches = defaultdict(list)     # same partition function as the kit: one server, one partition
            for r in recs:
                k, payload = to_state(r.value())
                batches[part_of(k)].append(SendMessage(payload))
            await asyncio.gather(*(iggy.send_messages(STREAM, STATE, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="bootstrap servers of the Kafka-protocol endpoint")
    p.add_argument("--topic", default="server-state", help="the topic that carries engine instances' state reports")
    p.add_argument("-X", action="append", default=[], metavar="key=value",
                   help="extra client setting, e.g. -X security.protocol=SASL_SSL")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
