"""Shadow your existing vehicle inventory change topic from Kafka/MSK into the vehicle lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the DMS services that publish inventory events, and every
existing consumer, are untouched. It writes, keyed by rooftop:VIN, the resulting state of each vehicle (status,
price, miles), so agent_context.py can run on your real traffic.

Map your records in to_change(): return (rooftop:VIN, {"status", "price", "miles"}) or None to skip a record.
Records must be keyed (or partitioned) by vehicle on your topic, so the bridge sees each vehicle's records in
order; it numbers them per vehicle as it forwards them. If your topic carries partial updates, the bridge merges
them into the vehicle's last known state before forwarding.

Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for SASL/SCRAM on MSK:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STREAM, VEHICLES, msg, part_of


def to_change(value: bytes):
    """Your record shape goes here. This default expects JSON like
    {"dealer_id": ..., "vin": ..., "status": "In Stock", "price": 31990, "miles": 12}."""
    d = json.loads(value)
    if "vin" not in d:
        return None
    keep = {f: d[f] for f in ("status", "price", "miles") if f in d}
    return f"{d['dealer_id']}:{d['vin']}", keep


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, VEHICLES, topic_partitions_count=PARTITIONS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True, "auto.commit.interval.ms": 1000}   # offsets once a second
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    state = defaultdict(lambda: {"status": "In Stock", "price": 0, "miles": 0})   # resulting state per vehicle
    seq = defaultdict(int)
    forwarded, t0 = 0, time.monotonic()
    async with producer:
        while True:
            batches = defaultdict(list)
            for rec in kafka.consume(num_messages=2000, timeout=0.005):
                if rec.error():
                    continue
                got = to_change(rec.value())
                if got is None:
                    continue
                k, fields = got
                state[k].update(fields)
                seq[k] += 1
                ns = rec.timestamp()[1] * 1_000_000 if a.ns_from_kafka else time.time_ns()
                batches[part_of(k)].append(SendMessage(msg(k=k, seq=seq[k], ns=ns, kind="shadow", **state[k])))
            if batches:
                # One batch per partition, all sent at once; a vehicle's records keep their order.
                await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in batches.items()))
                forwarded += sum(len(ms) for ms in batches.values())
            if time.monotonic() - t0 >= 1:
                print(f"[bridge] {forwarded:,} vehicle changes/s forwarded from '{a.kafka_topic}'", flush=True)
                forwarded, t0 = 0, time.monotonic()
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="MSK bootstrap brokers")
    p.add_argument("--kafka-topic", required=True, help="your vehicle inventory change topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--ns-from-kafka", action="store_true",
                   help="time latency from the Kafka record timestamp instead of from the bridge")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
