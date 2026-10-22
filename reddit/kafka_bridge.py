"""Shadow your preliminary content stream and ML signal topics from Kafka into the signal lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow") on the topics Signals-Joiner reads today: the
preliminary enriched content (JSON) and the ML signal topics. REV2, Signals-Joiner, the ML services and
every other consumer are untouched. Each record is mapped to the lane's shape and written keyed by content
ID with the lane's partition function, so signals_joiner.py and rev2_reader.py run on your traffic.

Map your records in to_content() and to_signal(). The defaults read content JSON with a top-level content
ID field (--id-field, e.g. a t3_/t1_ fullname) and decide which signals to expect from its kind and media
flag (common.expected); signal records are read as JSON with the same ID field and a score field. If your
signals are Protobuf, decode them in to_signal() with your generated classes. In a shadow run only the
lane is timed: the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SSL -X ssl.ca.location=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PRELIM_TOPIC, SIGNALS, STREAM, connect, ensure_all, expected, msg, part, signal_topic


def to_content(value: bytes, a) -> dict:
    d = json.loads(value)
    cid = str(d[a.id_field])
    kind = "post" if cid.startswith("t3_") else "comment"
    media = int(bool(d.get(a.media_field)))
    return dict(id=cid, seq=1, kind=kind, sr=str(d.get(a.subreddit_field, "")), media=media,
                exp=expected(kind, media))


def to_signal(value: bytes, sig: str, a) -> dict:
    d = json.loads(value)
    return dict(id=str(d[a.id_field]), sig=sig, score=float(d[a.score_field]))


async def main(a):
    iggy = await connect(a.url)
    parts = await ensure_all(iggy, a.partitions)
    sig_of = {}                                          # Kafka topic -> signal name
    for kv in a.signal_topic:
        s, t = kv.split("=", 1)
        if s not in SIGNALS:
            raise SystemExit(f"unknown signal {s}: add it to common.SIGNALS first")
        sig_of[t] = s
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.prelim_topic] + list(sig_of))
    while True:
        batches = defaultdict(lambda: defaultdict(list))  # lane topic -> partition -> messages
        for rec in kafka.consume(num_messages=1000, timeout=0.001):
            if rec.error():
                continue
            try:
                if rec.topic() == a.prelim_topic:
                    e, topic = to_content(rec.value(), a), PRELIM_TOPIC
                else:
                    e, topic = to_signal(rec.value(), sig_of[rec.topic()], a), signal_topic(sig_of[rec.topic()])
            except (KeyError, ValueError, TypeError):
                continue                                 # not a record this mapping understands
            batches[topic][part(e["id"], parts)].append(SendMessage(msg(**e)))
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, t, Partitioning.partition_id(q), ms)
                                   for t, by in batches.items() for q, ms in by.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    ap.add_argument("--prelim-topic", required=True, help="your preliminary enriched content topic")
    ap.add_argument("--signal-topic", action="append", default=[], metavar="signal=kafka_topic",
                    help="an ML signal topic, e.g. text_harm=<your topic>; repeat per signal")
    ap.add_argument("--id-field", default="content_id")
    ap.add_argument("--media-field", default="has_media")
    ap.add_argument("--subreddit-field", default="subreddit_id")
    ap.add_argument("--score-field", default="score")
    ap.add_argument("--partitions", type=int, default=32, help="partitions for new lane topics")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
