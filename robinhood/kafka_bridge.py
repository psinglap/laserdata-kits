"""Shadow an existing account-update topic from Kafka into Apache Iggy, read-only.

Reads as its own consumer group ("iggy-shadow") and never commits offsets, so producers and every existing
consumer, including the stand-in authorizer that reads the topic today, are untouched. Works with any Kafka API
endpoint: open-source Kafka or Confluent (pass SASL settings with -X).

Point standin.py at Iggy and compare. Map your records in to_change(): the kit needs k (account), seq (per
account, increasing), avail (spendable balance in cents) and status ("active" or not).

Needs: pip install apache-iggy==0.9.0 confluent-kafka
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, STATE, STREAM, connect, ensure_topics, part_of


def to_change(body: bytes) -> tuple[str, bytes]:
    d = json.loads(body)                    # map your field names here
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
            batches = defaultdict(list)     # same partition function as the kit: one account, one partition
            for r in recs:
                k, payload = to_change(r.value())
                batches[part_of(k)].append(SendMessage(payload))
            await asyncio.gather(*(iggy.send_messages(STREAM, STATE, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers")
    p.add_argument("--topic", default="account-updates", help="the topic the stand-in authorizer reads today")
    p.add_argument("-X", action="append", default=[], metavar="key=value",
                   help="extra Kafka client setting, e.g. -X security.protocol=SASL_SSL")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
