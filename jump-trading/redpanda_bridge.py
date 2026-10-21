"""Shadow your link telemetry from Redpanda into the Apache Iggy lane, read-only.

Redpanda speaks the Kafka protocol, so this is a plain Kafka consumer in its own group ("iggy-shadow"): the
collectors, every producer, the Redpanda cluster and everything that reads it today are untouched. For each record
it publishes one sample, keyed by link, with the link's sample sequence, the same shape links.py writes. Point
link_rules.py at Iggy and compare its alerts and timings with your production alerts.

Map your record in to_sample(): the kit needs the link ID, RSSI, SNR and BER. The defaults read JSON with fields
link_id, rssi_dbm, snr_db and ber. In a shadow run only the lane is timed; the clock starts when the bridge reads
the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-256 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, SAMPLES, STREAM, ensure_topics, msg, part


def to_sample(value: bytes):
    d = json.loads(value)
    return str(d["link_id"]), dict(rssi=float(d["rssi_dbm"]), snr=float(d["snr_db"]), ber=float(d["ber"]))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, SAMPLES)
    producer = await iggy.producer(STREAM, SAMPLES, topic_partitions_count=PARTITIONS[SAMPLES])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    redpanda = Consumer(conf)
    redpanda.subscribe([a.topic])
    seq = defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in redpanda.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    k, s = to_sample(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a link sample this mapping understands
                seq[k] += 1
                groups[part(k, PARTITIONS[SAMPLES])].append(SendMessage(msg(l=k, seq=seq[k], **s)))
            # One batch per partition, all sent at once, so a link's samples stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Redpanda brokers (Kafka API)")
    p.add_argument("--topic", default="link-telemetry", help="the topic your link samples land on")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
