"""Shadow your auction log from Kafka into the Apache Iggy outcome lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the exchange nodes, every producer and every
existing consumer (reporting, billing, the floor models you run today) are untouched. Each auction record
becomes one outcome, keyed by inventory segment, with a per-(node, segment) sequence: the same shape
exchange.py writes. Point floors.py at Iggy and compare its floors, and how fast they move, with yours.

Map your record in to_outcome(): the kit needs the exchange node (or any producer ID), the segment fields
(by default content category, device type and break slot; use any fields your floors are keyed by), the
number of bids that came back in time, and the top and second bid in USD CPM. The defaults read JSON with
fields host, content_category, device_type, break_slot, bid_count, top_bid_cpm and second_bid_cpm.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, OUTCOMES, PARTITIONS, STREAM, ensure_topics, msg, part


def to_outcome(value: bytes, a):
    d = json.loads(value)
    seg = "/".join(str(d.get(f, "unknown")) for f in a.segment_fields.split(","))
    return (str(d.get(a.node_field, "unknown")), seg, int(d.get(a.count_field, 0)),
            float(d.get(a.top_field) or 0), float(d.get(a.second_field) or 0))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, OUTCOMES)
    producer = await iggy.producer(STREAM, OUTCOMES, topic_partitions_count=PARTITIONS[OUTCOMES])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq = defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    x, s, n, top, second = to_outcome(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not an auction record this mapping understands
                seq[(x, s)] += 1
                groups[part(s, PARTITIONS[OUTCOMES])].append(
                    SendMessage(msg(x=x, s=s, seq=seq[(x, s)], n=n, top=round(top, 4), sec=round(second, 4))))
            # One batch per partition, all sent at once, so a segment's outcomes stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    p.add_argument("--kafka-topic", default="auction-log", help="your auction or bid log topic")
    p.add_argument("--node-field", default="host")
    p.add_argument("--segment-fields", default="content_category,device_type,break_slot")
    p.add_argument("--count-field", default="bid_count")
    p.add_argument("--top-field", default="top_bid_cpm")
    p.add_argument("--second-field", default="second_bid_cpm")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
