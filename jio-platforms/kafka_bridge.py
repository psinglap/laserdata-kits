"""Shadow your probe-record topic from Kafka into Apache Iggy, read-only.

For the case where your SDRs (or a copy of them) already land on a Kafka topic; your data platform job
posts list Kafka, Spark Streaming, Flink and NiFi. Runs as its own Kafka consumer group ("iggy-shadow"),
so the vProbes, the conductor fabric, ATOM and every existing consumer are untouched. Point anomaly.py at
Iggy and compare. If the records reach you another way (a NiFi flow, files, a socket), replace the
Kafka consumer below and keep to_msg().

Map your records in decode() and to_msg(): the kit needs k (the NF instance, for example "amf-mu-02"),
seq, proc (procedure), res ("ok" or "fail"), cause and dur (procedure time in ms). The default reads JSON so
the bridge runs as is; decode your own record format in decode(). Only the NF instance, procedure, result,
cause and duration cross the bridge: no subscriber identity (IMSI, MSISDN, IP) is copied into the lane.
Records keep the kit's key partitioning, so each instance's records stay in order.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for SASL:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, SDRS, STREAM, ensure_topics, msg, part

seq = defaultdict(int)


def decode(value: bytes) -> dict:
    return json.loads(value)


def to_msg(value: bytes):
    d = decode(value)
    # Rename your fields here: the NF instance that recorded the procedure, the procedure, and its outcome.
    k = d.get("k") or d.get("nf_instance") or f"{d.get('nf_type')}-{d.get('circle')}-{d.get('instance')}"
    seq[k] = max(seq[k] + 1, time.time_ns())  # rises across bridge restarts, so the check never drops new records
    res = d.get("res") or ("fail" if d.get("cause") else "ok")
    # ns = when the bridge read it, so the lane is timed from here
    return k, msg(k=k, seq=seq[k], pid=d.get("pid") or d.get("procedure_id") or "",
                  proc=d.get("proc") or d.get("procedure") or "", res=res, cause=d.get("cause") or "",
                  dur=d.get("dur") or d.get("duration_ms") or 0, deg=0)


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, SDRS)
    producer = await iggy.producer(STREAM, SDRS, topic_partitions_count=PARTITIONS[SDRS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if not rec.error():
                    k, m = to_msg(rec.value())
                    groups[part(k, PARTITIONS[SDRS])].append(SendMessage(m))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", required=True, help="your probe-record topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
