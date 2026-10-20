"""Shadow your impression events from Kafka into the Apache Iggy household lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so ad servers, every producer and every existing
consumer (Spark Streaming reporting, log-level data, billing) are untouched. For each impression it keeps a
running count per (ad server, household, campaign, cap window) and publishes the resulting total with a
sequence, keyed by household, the same shape adserver.py writes. Start adserver.py processes with
--rate 0 to read the lane only, and compare their household counts with your frequency store.

Map your record in to_impression(): the kit needs a household ID (or a device IFA if that is what you
cap on), the campaign or line item, and the ad server host. The defaults read JSON with fields
household_id, campaign_id and host. In a shadow run only the lane is timed: the clock starts when the
bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, EXPOSURES, PARTITIONS, STREAM, cap_window, connect, ensure_topics, msg, part


def to_impression(value: bytes, a):
    d = json.loads(value)
    return str(d[a.household_field]), str(d[a.campaign_field]), str(d.get(a.server_field, "unknown"))


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy, EXPOSURES)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq, n = defaultdict(int), defaultdict(int)
    P = PARTITIONS[EXPOSURES]
    while True:
        w = cap_window(a.window_s)
        batches = defaultdict(list)
        for rec in kafka.consume(num_messages=1000, timeout=0.001):
            if rec.error():
                continue
            try:
                h, c, s = to_impression(rec.value(), a)
            except (KeyError, ValueError):
                continue                    # not an impression record this mapping understands
            s = f"bridge:{s}"
            seq[(s, h)] += 1
            n[(s, h, c, w)] += 1
            batches[part(h, P)].append(SendMessage(msg(s=s, h=h, c=c, w=w, seq=seq[(s, h)], n=n[(s, h, c, w)])))
        # One batch per partition, all sent at once, so a household's impressions stay in order.
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, EXPOSURES, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        if len(n) > 2_000_000:
            for k in [k for k in n if k[-1] < w]:
                del n[k]
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    p.add_argument("--kafka-topic", default="impressions", help="your impression or tracking-beacon topic")
    p.add_argument("--household-field", default="household_id")
    p.add_argument("--campaign-field", default="campaign_id")
    p.add_argument("--server-field", default="host")
    p.add_argument("--window-s", type=float, default=86_400, help="cap window in seconds; a day by default")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
