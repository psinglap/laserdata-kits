"""Shadow the OMS's trade events from Redpanda into Apache Iggy, read-only.

Alpaca's OMS v2 writes its write-ahead log to Redpanda, and Alpaca's OMS launch post says other services can
consume trade and order events from that log in a decoupled way. This bridge is one more such consumer: it reads
one topic over the Kafka API as its own consumer group ("iggy-shadow") and never commits offsets, so the OMS, the
log and every existing consumer are untouched. Pass SASL or TLS settings with -X.

Each record is mapped into its partner's log on Iggy, keyed by account ID with the same partition function as the
kit. Map your fields in to_event(): the kit needs corr (the partner's correspondent code), k (account ID), seq
(per account, increasing), event_id, event, and ns (creation time in ns; the bridge's own clock if absent).
Then point event_gateway.py at Iggy and compare with what your partners receive today.

Needs: pip install apache-iggy==0.9.0 confluent-kafka
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import CODES, DEFAULT_URL, PARTS, STREAM, connect, ensure_topics, part_of, topic, ulid


def to_event(body: bytes) -> tuple[str, str, bytes]:
    d = json.loads(body)                    # map your field names here
    d.setdefault("ns", time.time_ns())      # when the bridge saw it, if the record has no creation time in ns
    d.setdefault("event_id", ulid(d["ns"] // 1_000_000))
    return d["corr"], d["k"], json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    from confluent_kafka import Consumer

    iggy = await connect(a.url)
    await ensure_topics(iggy)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": False}    # leave no committed offsets behind
    conf.update(kv.split("=", 1) for kv in a.X)
    rp = Consumer(conf)
    rp.subscribe([a.topic])
    while True:
        recs = [r for r in rp.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            batches = defaultdict(list)     # one batch per (partner, partition): an account stays in order
            for r in recs:
                corr, k, payload = to_event(r.value())
                if corr in CODES:
                    batches[(corr, part_of(k, PARTS[corr]))].append(SendMessage(payload))
            await asyncio.gather(*(iggy.send_messages(STREAM, topic(c), Partitioning.partition_id(p), ms)
                                   for (c, p), ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Redpanda brokers (Kafka API)")
    p.add_argument("--topic", default="oms-trade-events", help="a topic of trade events you run today")
    p.add_argument("-X", action="append", default=[], metavar="key=value",
                   help="extra client setting, e.g. -X security.protocol=SASL_SSL")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
