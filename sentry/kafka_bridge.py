"""Shadow your ingest-monitors topic into the Crons check-in lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so Relay, the monitors consumer and every other
consumer are untouched. Each record on ingest-monitors is a msgpack wrapper written by Relay (a check_in with
the check-in JSON in "payload", or a clock_pulse). For a check-in it forwards the monitor slug, status,
check_in_id and monitor_config, keyed by monitor; for a clock pulse it sends one pulse into every lane
partition. monitors.py can then run its clock on your real traffic, next to the one you run today.

The lane's clock uses Iggy's append time, which here is your Kafka time plus the bridge's own delay; pass
--ns-from-kafka to time latency from the Kafka record timestamp instead of from the bridge.

Needs: pip install confluent-kafka msgpack apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SSL
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

import msgpack
from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import CHECKINS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg, part


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, CHECKINS)
    lane = await iggy.producer(STREAM, CHECKINS, topic_partitions_count=PARTITIONS[CHECKINS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True, "auto.commit.interval.ms": 1000}   # offsets once a second
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    P = PARTITIONS[CHECKINS]
    forwarded, pulses, t0 = 0, 0, time.monotonic()
    async with lane:
        while True:
            batches = defaultdict(list)
            for rec in kafka.consume(num_messages=5000, timeout=0.005):
                if rec.error():
                    continue
                w = msgpack.unpackb(rec.value(), raw=False)
                ns = rec.timestamp()[1] * 1_000_000 if a.ns_from_kafka else time.time_ns()
                if w.get("message_type") == "clock_pulse":
                    for p in range(P):
                        batches[p].append(SendMessage(msg(type="clock_pulse", partition_ids=list(range(P)), ns=ns)))
                    pulses += 1
                    continue
                c = json.loads(w["payload"])
                slug = f"{w['project_id']}/{c['monitor_slug']}"     # slugs are unique per project
                batches[part(slug, P)].append(SendMessage(msg(
                    type="check_in", monitor_slug=slug, status=c.get("status"), check_in_id=c.get("check_in_id"),
                    monitor_config=c.get("monitor_config"), ns=ns)))
                forwarded += 1
            if batches:
                # One batch per partition, all sent at once; a monitor's check-ins keep their order.
                await asyncio.gather(*(lane.send_with_partitioning(ms, Partitioning.partition_id(p))
                                       for p, ms in batches.items()))
            now = time.monotonic()
            if now - t0 >= 1:
                print(f"[bridge] {forwarded:,} check-ins/s and {pulses} clock pulses forwarded from "
                      f"'{a.kafka_topic}'", flush=True)
                forwarded, pulses, t0 = 0, 0, now
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", default="ingest-monitors", help="your check-in topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--ns-from-kafka", action="store_true",
                   help="time latency from the Kafka record timestamp instead of from the bridge")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
