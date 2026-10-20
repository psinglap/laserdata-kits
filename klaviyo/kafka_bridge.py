"""Shadow your failed writes, or a share of your live events, into the Apache Iggy failed-writes topic, read-only.

Joins your Kafka cluster as its own consumer group ("iggy-shadow"), so the publishing clients,
the producer proxy, the event processors and the retrier you run today are untouched. Two ways
to use it:
  * point it at your failed-writes topic on the self-hosted cluster: every real failure is copied;
  * point it at the priority topics on MSK with --share 0.5: half of the live events are copied,
    as if half of all publishes had failed, which is the surge to rehearse.
Then run: python retrier.py  (it re-publishes nothing to MSK; republish() is a stand-in) and compare.

Needs: pip install confluent-kafka apache-iggy==0.9.0
Map your records in to_event(): the kit needs the profile, the priority lane, the event name and id.
Pass client settings with -X, for example: -X security.protocol=SASL_SSL -X sasl.mechanism=AWS_MSK_IAM
"""
import argparse
import asyncio
import json
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer as KafkaConsumer

from common import DEFAULT_URL, FAILED, PRIORITIES, STREAM, connect, ensure_topic, msg, part


def to_event(topic: str, value: bytes, lanes: dict):
    """Return (profile id, lane, event name, event id) from one of your records, or None to skip it."""
    d = json.loads(value)
    profile = d.get("profile_id") or d.get("customer_id")
    lane = lanes.get(topic) or d.get("priority")            # one topic per lane: the topic names the lane
    if not profile or lane not in PRIORITIES:
        return None
    return str(profile), lane, str(d.get("metric") or d.get("event", "")), str(d.get("id", ""))


async def main(a):
    iggy = await connect(a.url)
    await ensure_topic(iggy, STREAM, FAILED, a.partitions)
    parts = (await iggy.get_topic(STREAM, FAILED)).partitions_count
    lanes = dict(kv.split("=", 1) for kv in a.lane)
    kafka = KafkaConsumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow",
                           "auto.offset.reset": "latest", "enable.auto.commit": True,
                           **dict(kv.split("=", 1) for kv in a.X)})
    kafka.subscribe(a.kafka_topics)
    seq, stats = defaultdict(int), defaultdict(int)
    mark = time.monotonic()
    while True:
        batches = defaultdict(list)
        for rec in kafka.consume(num_messages=5000, timeout=0.005):
            if rec.error() or random.random() >= a.share:
                continue
            ev = to_event(rec.topic(), rec.value(), lanes)
            if ev is None:
                continue
            profile, lane, name, eid = ev
            pid = part(profile, parts)
            seq[pid] += 1                                  # per-partition order as read; restart the bridge = new run
            ns = time.time_ns()
            batches[pid].append(SendMessage(msg(ns=ns, t0=ns, id=eid, pf=profile, p=lane, e=name, a=1,
                                                src="bridge", seq=seq[pid], why="shadow", bad=0)))
            stats["copied"] += 1
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, FAILED, Partitioning.partition_id(pid), ms)
                                   for pid, ms in batches.items()))
        if time.monotonic() - mark >= 1:
            print(f"[bridge] {stats['copied']:,} events/s copied into '{FAILED}'", flush=True)
            stats.clear()
            mark = time.monotonic()
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--kafka-topics", nargs="+", required=True, help="your failed-writes topic, or your lane topics")
    p.add_argument("--lane", action="append", default=[], metavar="topic=P10", help="which lane a topic carries")
    p.add_argument("--share", type=float, default=1.0, help="share of records to copy (0.5 = half of all events)")
    p.add_argument("--partitions", type=int, default=16, help="partitions for a new failed-writes topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
