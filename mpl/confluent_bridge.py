"""Shadow an existing Confluent Cloud topic of game results into Apache Iggy, read-only.

Reads as its own consumer group ("iggy-shadow") and never commits offsets, so the game servers that produce the
topic, the Flink jobs, the feature store pipeline and every existing consumer are untouched.

Point matchmaker.py at Iggy and compare. Map your records in to_form(): the kit needs k (player), seq (per
player, increasing), r (rating after the game), streak (wins in a row positive, losses in a row negative) and
games. If your topic carries raw outcomes rather than the resulting form, compute the form here, per player, from
the outcomes in order.

Needs: pip install apache-iggy==0.9.0 confluent-kafka
For Confluent Cloud pass the API key as SASL settings:
  -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN -X sasl.username=<api-key> -X sasl.password=<api-secret>
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, FORM, STREAM, connect, ensure_topics, part_of


def to_form(body: bytes) -> tuple[str, bytes]:
    d = json.loads(body)                    # map your field names here
    d.setdefault("ns", time.time_ns())      # when the bridge saw it, if the record has no creation time in ns
    return d["k"], json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    from confluent_kafka import Consumer

    iggy = await connect(a.url)
    await ensure_topics(iggy, FORM)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": False}    # leave no committed offsets behind
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    while True:
        recs = [r for r in kafka.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            batches = defaultdict(list)     # same partition function as the kit: one player, one partition
            for r in recs:
                k, payload = to_form(r.value())
                batches[part_of(k)].append(SendMessage(payload))
            await asyncio.gather(*(iggy.send_messages(STREAM, FORM, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Confluent Cloud bootstrap server")
    p.add_argument("--topic", default="game-results", help="the topic that carries finished games per player")
    p.add_argument("-X", action="append", default=[], metavar="key=value",
                   help="extra client setting, e.g. -X security.protocol=SASL_SSL")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
