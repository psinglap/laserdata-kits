"""Shadow your normalized vehicle telemetry topic from MSK into the vehicle state lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the telematics sync service, every producer, the
Flink jobs and every existing consumer are untouched. It keeps only the signals the app shows and writes,
keyed by vehicle, the resulting state of those signals, so vehicle_state.py can run on your real traffic.

Map your records in to_state(): return (vehicle ID, {signal: value}) for a record that carries any app
signal, or None to skip it. Records must be keyed (or partitioned) by vehicle on your topic, so the bridge
sees each vehicle's records in order; it numbers them per vehicle as it forwards them.

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

from common import APP_SIGNALS, DEFAULT_URL, PARTITIONS, STATE, STREAM, ensure_topics, msg, part


def to_state(value: bytes):
    """Your record shape goes here. This default expects JSON like {"vehicle_id": ..., "signals": {...}}."""
    d = json.loads(value)
    sig = d.get("signals", d)
    keep = {s: sig[s] for s in APP_SIGNALS if s in sig}
    return (d["vehicle_id"], keep) if keep else None


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, STATE)
    producer = await iggy.producer(STREAM, STATE, topic_partitions_count=PARTITIONS[STATE])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True, "auto.commit.interval.ms": 1000}   # offsets once a second
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    current = defaultdict(dict)             # vehicle -> latest value of each app signal (resulting state)
    seq = defaultdict(int)
    forwarded, t0 = 0, time.monotonic()
    async with producer:
        while True:
            batches = defaultdict(list)
            for rec in kafka.consume(num_messages=2000, timeout=0.005):
                if rec.error():
                    continue
                got = to_state(rec.value())
                if got is None:
                    continue                # no app signal in this record: the lane never carries it
                v, signals = got
                current[v].update(signals)
                seq[v] += 1
                ns = rec.timestamp()[1] * 1_000_000 if a.ns_from_kafka else time.time_ns()
                batches[part(v, PARTITIONS[STATE])].append(
                    SendMessage(msg(v=v, seq=seq[v], ns=ns, **current[v])))
            if batches:
                # One batch per partition, all sent at once; a vehicle's records keep their order.
                await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in batches.items()))
                forwarded += sum(len(ms) for ms in batches.values())
            if time.monotonic() - t0 >= 1:
                print(f"[bridge] {forwarded:,} vehicle states/s forwarded from '{a.kafka_topic}'", flush=True)
                forwarded, t0 = 0, time.monotonic()
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="MSK bootstrap brokers")
    p.add_argument("--kafka-topic", required=True, help="your normalized telemetry topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--ns-from-kafka", action="store_true",
                   help="time latency from the Kafka record timestamp instead of from the bridge")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
