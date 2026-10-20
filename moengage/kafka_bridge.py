"""Shadow trigger events from an existing Kafka topic into per-workspace Apache Iggy topics, read-only.

Joins your Kafka cluster as its own consumer group ("iggy-shadow"), so producers, the
Samza and Flink jobs and every existing consumer are untouched. It keeps only the events
that live Smart Trigger campaigns and Flows wait on (--events), and writes each one to its
workspace's topic, keyed by user. Point trigger_eval.py at Iggy and compare.

Uses kafka-python, the client MoEngage already maintains a fork of.
Needs: pip install kafka-python apache-iggy==0.9.0
Map your records in to_event(): the kit needs the workspace, the user ID and the event name.
Pass client settings with -X, for example: -X security_protocol=SASL_SSL -X sasl_mechanism=SCRAM-SHA-512
"""
import argparse
import asyncio
import json
import re
import time
import zlib
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from kafka import KafkaConsumer

from common import DEFAULT_URL, EVENTS, STREAM, connect, ensure_topic, msg


def to_event(value: bytes):
    """Return (workspace, user, event name, event time ns) from one of your records, or None to skip it."""
    d = json.loads(value)
    ws, user, name = d.get("workspace_id") or d.get("app_id"), d.get("user_id") or d.get("uid"), d.get("event")
    if not (ws and user and name):
        return None
    return str(ws), str(user), name, int(d.get("ts_ns") or time.time_ns())


async def main(a):
    iggy = await connect(a.url)
    wanted = set(a.events)
    kafka = KafkaConsumer(a.kafka_topic, bootstrap_servers=a.bootstrap, group_id="iggy-shadow",
                          auto_offset_reset="latest", enable_auto_commit=True,
                          **{k: v for k, v in (kv.split("=", 1) for kv in a.X)})
    topics, seq, stats = {}, defaultdict(int), defaultdict(int)     # workspace -> (topic, partitions)
    mark = time.monotonic()
    while True:
        batches = defaultdict(list)
        for records in kafka.poll(timeout_ms=5, max_records=5000).values():
            for rec in records:
                ev = to_event(rec.value)
                if ev is None or ev[2] not in wanted:
                    continue
                ws, user, name, ns = ev
                if ws not in topics:
                    t = "ws-" + re.sub(r"[^A-Za-z0-9_.-]", "_", ws)[:200]
                    await ensure_topic(iggy, STREAM, t, a.partitions)
                    topics[ws] = (t, (await iggy.get_topic(STREAM, t)).partitions_count)
                t, parts = topics[ws]
                seq[ws, user] += 1               # per-user order as read from Kafka; restart the bridge = new run
                batches[t, zlib.crc32(user.encode()) % parts].append(
                    SendMessage(msg(ns=ns, ws=ws, u=user, seq=seq[ws, user], e=name)))
                stats["copied"] += 1
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, t, Partitioning.partition_id(pid), ms)
                                   for (t, pid), ms in batches.items()))
        if time.monotonic() - mark >= 1:
            print(f"[bridge] {stats['copied']:,} trigger events/s copied into {len(topics)} workspace topics", flush=True)
            stats.clear()
            mark = time.monotonic()
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", required=True, help="the topic your trigger evaluation reads today")
    p.add_argument("--events", nargs="+", default=[e for e, _ in EVENTS], help="event names live campaigns trigger on")
    p.add_argument("--partitions", type=int, default=1, help="partitions for a new workspace topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra KafkaConsumer setting")
    asyncio.run(main(p.parse_args()))
