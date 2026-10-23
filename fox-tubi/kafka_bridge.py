"""Shadow your bid or ad-request log from Kafka into the Apache Iggy creative-seen lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the ad servers, every producer and every existing
consumer (reporting, billing, the transcoding and brand recognition you run today) are untouched. The first
time a node's records show a creative, it becomes one sighting, keyed by creative, with a per-node sequence:
the same shape ad_servers.py writes. Point creative_registry.py at Iggy and compare how many jobs it starts,
and how many sightings it absorbs, with how many your pipeline starts today.

Map your record in to_sighting(): the kit needs the ad server node (or any producer ID), the creative ID
(OpenRTB bid.crid, or your own creative key) and the DSP. The defaults read JSON with fields host, crid and
dsp. Without a creative registry of your own to compare against, pass --known with a file of creative IDs
already approved (one per line), so only new creatives are sent.
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

from common import DEFAULT_URL, PARTITIONS, SEEN, STREAM, ensure_topics, msg, part


def to_sighting(value: bytes, a):
    d = json.loads(value)
    return str(d.get(a.node_field, "unknown")), str(d[a.creative_field]), str(d.get(a.dsp_field, "unknown"))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, SEEN)
    producer = await iggy.producer(STREAM, SEEN, topic_partitions_count=PARTITIONS[SEEN])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    known = set(open(a.known).read().split()) if a.known else set()
    seen = defaultdict(set)                 # node -> creatives already sent for that node
    seq = defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    x, c, d = to_sighting(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not a bid record this mapping understands
                if c in known or c in seen[x]:
                    continue
                seen[x].add(c)
                seq[x] += 1
                groups[part(c, PARTITIONS[SEEN])].append(SendMessage(msg(x=x, c=c, d=d, seq=seq[x])))
            # One batch per partition, all sent at once, so a creative's sightings stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    p.add_argument("--kafka-topic", default="ad-requests", help="your bid or ad-request log topic")
    p.add_argument("--node-field", default="host")
    p.add_argument("--creative-field", default="crid")
    p.add_argument("--dsp-field", default="dsp")
    p.add_argument("--known", default="", help="file of creative IDs already approved, one per line")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
