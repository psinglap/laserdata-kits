"""Shadow your win or spend events from Kafka into the Apache Iggy spend lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the bid servers, every producer and every
existing consumer (pacing, reporting, billing) are untouched. For each win it keeps a running spend per
(bid server, ad group, flight) and publishes the resulting total with a sequence, keyed by ad group, the
same shape bidders.py writes. Point pacing.py at Iggy and compare its pause decisions with yours.

Map your record in to_win(): the kit needs the ad group, the bid server (or any producer ID), the
clearing price in USD for this impression, and optionally the event time. The defaults read JSON with
fields ad_group_id, host and price_usd; a CPM field can be converted with --cpm-field.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, SPEND, STREAM, ensure_topics, flight, msg, part


def to_win(value: bytes, a):
    d = json.loads(value)
    price = float(d[a.cpm_field]) / 1000 if a.cpm_field else float(d.get(a.price_field, 0))
    return str(d[a.group_field]), str(d.get(a.server_field, "unknown")), price


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, SPEND)
    producer = await iggy.producer(STREAM, SPEND, topic_partitions_count=PARTITIONS[SPEND])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq, cum = defaultdict(int), defaultdict(float)
    async with producer:
        while True:
            f = flight(a.flight_s)
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    g, b, price = to_win(rec.value(), a)
                except (KeyError, ValueError):
                    continue                # not a win record this mapping understands
                seq[(b, g)] += 1
                cum[(f, b, g)] += price
                groups[part(g, PARTITIONS[SPEND])].append(
                    SendMessage(msg(b=b, g=g, flight=f, seq=seq[(b, g)], spend=round(cum[(f, b, g)], 6))))
            # One batch per partition, all sent at once, so an ad group's spend stays in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            for k in [k for k in cum if k[0] < f - 1] if len(cum) > 1_000_000 else []:
                del cum[k]
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    p.add_argument("--kafka-topic", default="wins", help="your win-notification or spend topic")
    p.add_argument("--group-field", default="ad_group_id")
    p.add_argument("--server-field", default="host")
    p.add_argument("--price-field", default="price_usd", help="clearing price of this impression in USD")
    p.add_argument("--cpm-field", default="", help="or a CPM field, divided by 1,000")
    p.add_argument("--flight-s", type=float, default=86_400, help="budget period in seconds; a day by default")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
