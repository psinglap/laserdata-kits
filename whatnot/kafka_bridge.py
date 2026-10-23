"""Shadow your story events from Kafka into the Apache Iggy lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the clients, the events ingestion service, every
producer, the ClickHouse sink and the minute worker are untouched. For each story event it publishes one record,
keyed by story key, the same shape live_shows.py writes; everything else on the topic is skipped. Point
story_judge.py at Iggy and compare its verdicts, SLIs and alert times with your E2E SLO dashboards and pages.

Map your record in to_event(): the kit needs the story key, the story type, the role (trigger, step, outcome or
expected-failure name, as in common.STORIES), the client ID and its surface, and the event's client time. The
defaults read JSON with fields story_key, story, role, client_id, platform and client_ts_ms; if yours are
protobuf, decode them here. By default each record keeps the client's own time, so deadlines are judged as your
minute worker judges them and event-to-judge includes your whole ingestion path; with --lane-clock the record is
stamped when the bridge reads it, which times the lane alone (deadlines then run from that moment).
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example (Confluent Cloud):
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=<api key> -X sasl.password=<secret>
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STORIES, STORY_EVENTS, STREAM, ensure_topics, msg, part

KNOWN = {(st, r) for st, s in STORIES.items() for r in [s["trigger"], *s["steps"], *s["outcomes"], *s["expected"]]}


def to_event(value: bytes):
    d = json.loads(value)
    st, r = str(d["story"]), str(d["role"])
    if (st, r) not in KNOWN:
        return None
    return str(d["story_key"]), dict(st=st, r=r, c=str(d["client_id"]), sf=str(d["platform"]),
                                     ts=int(d["client_ts_ms"]))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, STORY_EVENTS)
    producer = await iggy.producer(STREAM, STORY_EVENTS, topic_partitions_count=PARTITIONS[STORY_EVENTS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    r = to_event(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a record this mapping understands
                if r is None:
                    continue                # not a story event: analytics and the rest stay on Kafka only
                key, ev = r
                # ns is the clock the judge uses for latency and deadlines: the client's time, or now (--lane-clock).
                ev["ns"] = time.time_ns() if a.lane_clock else ev["ts"] * 1_000_000
                groups[part(key, PARTITIONS[STORY_EVENTS])].append(SendMessage(msg(k=key, **ev)))
            # One batch per partition, all sent at once, so a story's events stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="story-events", help="the topic your events ingestion writes stories to")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--lane-clock", action="store_true",
                   help="stamp records when read (times the lane alone) instead of with the client's time")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
