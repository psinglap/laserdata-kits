"""Shadow finished views from your Kafka topic into per-export Apache Iggy topics, read-only.

Mux's processors send finished views on to "another Kafka cluster" before the database, and Kafka
Connect reads the export streams from Kafka today. This bridge joins that cluster as its own
consumer group ("iggy-shadow"), so the collectors, the processors, the Kafka Connect sinks and every
existing consumer are untouched. For each finished view it looks up whether that environment has a
streaming export (exports.json, the same shape as Settings > Streaming Exports) and writes it to
that export's topic, keyed by view ID, with a sequence per export partition. Then run:
python exporter.py --envs 0   (reads every export topic in the stream) and compare its lines with
what your Kafka Connect sinks deliver.

Needs: pip install confluent-kafka apache-iggy==0.9.0
Map your records in to_view(): the kit needs the environment, the view ID and a version.
Pass client settings with -X, for example: -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512
If your records are protobuf VideoView messages, decode them in to_view() with the mux-protobuf classes.

exports.json:
  {"exports": [{"name": "env-acme-prod-kinesis", "environment_id": "<environment id>"},
               {"name": "env-other-pubsub", "environment_id": "<environment id>", "partitions": 4}]}
"""
import argparse
import asyncio
import json
import re
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer as KafkaConsumer

from common import DEFAULT_URL, STREAM, connect, ensure_topic, msg, part


def to_view(value: bytes):
    """Return (environment id, view id, version, finished time ns) from one of your records, or None to skip it."""
    d = json.loads(value)
    env = d.get("environment_id") or d.get("property_id")
    view = d.get("view_id") or d.get("id")
    if not (env and view):
        return None
    ver = int(d.get("version") or d.get("view_version") or 1)
    return str(env), str(view), ver, int(d.get("ts_ns") or time.time_ns())


async def main(a):
    iggy = await connect(a.url)
    routes = {}                                          # environment id -> (topic, partitions)
    for e in json.load(open(a.config))["exports"]:
        t = re.sub(r"[^A-Za-z0-9_.-]", "_", e["name"])[:200]
        await ensure_topic(iggy, STREAM, t, int(e.get("partitions", a.partitions)))
        routes[str(e["environment_id"])] = (t, (await iggy.get_topic(STREAM, t)).partitions_count)
    kafka = KafkaConsumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow",
                           "auto.offset.reset": "latest", "enable.auto.commit": True,
                           **dict(kv.split("=", 1) for kv in a.X)})
    kafka.subscribe(a.kafka_topics)
    seq, stats = defaultdict(int), defaultdict(int)
    mark = time.monotonic()
    print(f"[bridge] reading {', '.join(a.kafka_topics)} as group iggy-shadow, read-only, into {len(routes)} export topics",
          flush=True)
    while True:
        batches = defaultdict(list)
        for rec in kafka.consume(num_messages=5000, timeout=0.005):
            if rec.error():
                continue
            v = to_view(rec.value())
            if v is None:
                continue
            env, view, ver, ns = v
            if env not in routes:
                continue                                 # this environment has no streaming export
            t, parts = routes[env]
            pid = part(view, parts)
            seq[t, pid] += 1                             # order as read from Kafka; restart the bridge = new run
            batches[t, pid].append(SendMessage(msg(ns=ns, view_id=view, ver=ver, seq=seq[t, pid])))
            stats["copied"] += 1
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, t, Partitioning.partition_id(pid), ms)
                                   for (t, pid), ms in batches.items()))
        if time.monotonic() - mark >= 1:
            print(f"[bridge] {stats['copied']:,} views/s copied into {len(routes)} export topics", flush=True)
            stats.clear()
            mark = time.monotonic()
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers of the finished-views cluster")
    p.add_argument("--kafka-topics", nargs="+", required=True, help="your finished-view topics")
    p.add_argument("--config", required=True, help="exports.json: which environments have a streaming export")
    p.add_argument("--partitions", type=int, default=1, help="partitions for a new export topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
