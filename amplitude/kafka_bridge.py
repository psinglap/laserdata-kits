"""Shadow your raw event topic from Kafka into the Apache Iggy device-counts lane, read-only.

Runs as its own consumer group ("iggy-shadow"), so the SDK endpoints, the ingestion pipeline and Nova are
untouched. For each event it reads the device (and the event time, if present) and publishes, once per 10 ms,
the running count of events per device per second, with a sequence, keyed by device: the same shape
ingest_nodes.py writes, as if the bridge were one more ingestion node. Point throttle.py at Iggy and compare its
throttles with the 429s your endpoints return today (throttled_devices).

Map your record in to_device(): the kit needs the device ID (or the user ID, to test the per-user limit).
The defaults read JSON with a field device_id. In a shadow run only the lane is timed; the clock starts when the
bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example (MSK with IAM or SASL):
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import COUNTS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg, part


def to_device(value: bytes, a):
    d = json.loads(value)
    return str(d[a.device_field])


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, COUNTS)
    producer = await iggy.producer(STREAM, COUNTS, topic_partitions_count=PARTITIONS[COUNTS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    seq, cur = defaultdict(int), {}
    async with producer:
        while True:
            sec = int(time.time())
            out = {}
            for rec in kafka.consume(num_messages=5000, timeout=0.01):
                if rec.error():
                    continue
                try:
                    d = to_device(rec.value(), a)
                except (KeyError, ValueError):
                    continue                # not an event this mapping understands
                c = cur.get(d)
                if not c or c[0] != sec:
                    c = cur[d] = [sec, 0]
                c[1] += 1
                out[d] = c[1]
            groups = defaultdict(list)
            for d, cnt in out.items():
                seq[d] += 1
                groups[part(d, PARTITIONS[COUNTS])].append(SendMessage(msg(n=a.node, d=d, s=sec, c=cnt, q=seq[d])))
            # One batch per partition, all sent at once, so a device's counts stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))
            if len(cur) > 2_000_000:
                for d in [d for d, c in cur.items() if c[0] < sec - 60]:
                    del cur[d]
            await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="your Kafka brokers, host:port")
    ap.add_argument("--topic", default="events", help="your raw event topic")
    ap.add_argument("--device-field", default="device_id", help="or user_id, for the per-user limit")
    ap.add_argument("--node", default="kafka-bridge", help="how this bridge names itself in the counts")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
