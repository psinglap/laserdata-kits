"""Shadow the mark prices you already publish internally from Kafka into Apache Iggy, read-only.

Reads as its own consumer group ("iggy-shadow") and never commits offsets, so the feed handler and every existing
consumer, including the risk services that read the topic today, are untouched. Works with any Kafka API
endpoint (pass SASL or TLS settings with -X).

Point liquidator.py at Iggy and compare. Map your records in to_mark(): the kit needs k (the instrument, e.g.
B-BTC_USDT), seq (per instrument, increasing; the partner's mark send time bmST works), mp (mark price), and b and
n (batch number and instruments in the batch; bmST and your instrument count work).

Needs: pip install apache-iggy==0.9.0 confluent-kafka
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, MARKS, PARTITIONS, STREAM, connect, ensure_topics, part_of


def to_mark(body: bytes, n: int) -> tuple[str, bytes]:
    d = json.loads(body)                    # map your field names here
    out = {"k": d["k"], "seq": d.get("seq", d.get("bmST")), "mp": d.get("mp"), "b": d.get("b", d.get("bmST")),
           "n": d.get("n", n), "ns": d.get("ns") or time.time_ns()}   # when the bridge saw it, if no time in ns
    return out["k"], json.dumps(out, separators=(",", ":")).encode()


async def main(a):
    from confluent_kafka import Consumer

    iggy = await connect(a.url)
    await ensure_topics(iggy, MARKS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": False}    # leave no committed offsets behind
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    while True:
        recs = [r for r in kafka.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            batches = defaultdict(list)     # same partition function as the kit: one instrument, one partition
            for r in recs:
                k, payload = to_mark(r.value(), a.instruments)
                batches[part_of(k, PARTITIONS[MARKS])].append(SendMessage(payload))
            await asyncio.gather(*(iggy.send_messages(STREAM, MARKS, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers")
    p.add_argument("--topic", default="futures-mark-prices", help="the topic your risk services read marks from")
    p.add_argument("--instruments", type=int, default=505, help="instruments in each mark batch")
    p.add_argument("-X", action="append", default=[], metavar="key=value",
                   help="extra Kafka client setting, e.g. -X security.protocol=SASL_SSL")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
