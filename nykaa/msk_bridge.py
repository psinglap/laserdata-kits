"""Shadow your ad events from Amazon MSK into the Apache Iggy ad counter lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the API Gateway and Lambda ingest, the Managed Flink
jobs, MSK Connect into OpenSearch and every other consumer are untouched. For each impression it keeps a running
count per (shopper, campaign, window), and for each click a running count per (shopper, clicked campaign,
window), and publishes the resulting count with a sequence, keyed by shopper: the same shape shoppers.py writes.
Start selector.py processes to read the lane and compare their per-shopper counts with your ad counter.

Map your record in to_event(): the kit needs a shopper or device ID (whatever you count on), the event type and
the campaign. The defaults read JSON with fields user_id, event and campaign_id, where event is "impression" or
"click". In a shadow run only the lane is timed: the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for SASL/SCRAM on MSK:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, EVENTS, PARTITIONS, STREAM, clicked, connect, ensure_topics, msg, part, window


def to_event(value: bytes, a):
    d = json.loads(value)
    ev = str(d[a.event_field]).lower()
    c = str(d[a.campaign_field])
    if ev == a.impression_value:
        return str(d[a.user_field]), "imp", c
    if ev == a.click_value:
        return str(d[a.user_field]), "clk", clicked(c)
    raise ValueError(ev)                    # another event type (add to bag, order, ...): not on this lane


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy, EVENTS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    c = "msk-bridge"
    seq, n = defaultdict(int), defaultdict(int)
    P = PARTITIONS[EVENTS]
    while True:
        w = window(a.window_s)
        batches = defaultdict(list)
        for rec in kafka.consume(num_messages=1000, timeout=0.001):
            if rec.error():
                continue
            try:
                u, t, k = to_event(rec.value(), a)
            except (KeyError, ValueError):
                continue                    # not an ad event this mapping understands
            seq[u] += 1
            n[(u, k, w)] += 1
            batches[part(u, P)].append(SendMessage(msg(c=c, u=u, t=t, k=k, w=w, seq=seq[u], n=n[(u, k, w)])))
        # One batch per partition, all sent at once, so a shopper's events stay in order.
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, EVENTS, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        if len(n) > 2_000_000:
            for k in [k for k in n if k[-1] < w]:
                del n[k]
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your MSK bootstrap brokers")
    p.add_argument("--kafka-topic", default="ad-events", help="your MSK topic with ad impressions and clicks")
    p.add_argument("--user-field", default="user_id")
    p.add_argument("--event-field", default="event")
    p.add_argument("--impression-value", default="impression")
    p.add_argument("--click-value", default="click")
    p.add_argument("--campaign-field", default="campaign_id")
    p.add_argument("--window-s", type=float, default=86_400, help="counting window in seconds; a day by default")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
