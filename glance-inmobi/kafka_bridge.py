"""Shadow your ad beacons from Kafka into the Apache Iggy ad-state lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the beacon collectors, every producer and every
existing consumer (revenue events, partner reconciliation, Spark and Flink jobs) are untouched. For each
impression it keeps a running count per (user, campaign, cap window), and for each dismiss a running count
per (user, advertiser, cap window), and publishes the resulting total with a sequence, keyed by user: the same
shape beacons.py writes. Start adserver.py processes to read the lane and compare their per-user view with
your frequency store.

Map your record in to_beacon(): the kit needs a user or device ID (whatever you cap on), the event type, the
campaign and the advertiser. The defaults read JSON with fields user_id, event, campaign_id and advertiser_id,
where event is "impression" or "dismiss". In a shadow run only the lane is timed: the clock starts when the
bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, SIGNALS, STREAM, cap_window, connect, ensure_topics, msg, part


def to_beacon(value: bytes, a):
    d = json.loads(value)
    ev = str(d[a.event_field]).lower()
    if ev == a.impression_value:
        return str(d[a.user_field]), "imp", "c:" + str(d[a.campaign_field])
    if ev == a.dismiss_value:
        return str(d[a.user_field]), "dis", "a:" + str(d[a.advertiser_field])
    raise ValueError(ev)                    # another beacon type (click, quartile, ...): not on this lane


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy, SIGNALS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    s = "bridge"
    seq, n = defaultdict(int), defaultdict(int)
    P = PARTITIONS[SIGNALS]
    while True:
        w = cap_window(a.window_s)
        batches = defaultdict(list)
        for rec in kafka.consume(num_messages=1000, timeout=0.001):
            if rec.error():
                continue
            try:
                u, t, k = to_beacon(rec.value(), a)
            except (KeyError, ValueError):
                continue                    # not a beacon this mapping understands
            seq[u] += 1
            n[(u, k, w)] += 1
            batches[part(u, P)].append(SendMessage(msg(s=s, u=u, t=t, k=k, w=w, seq=seq[u], n=n[(u, k, w)], held=0)))
        # One batch per partition, all sent at once, so a user's beacons stay in order.
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, SIGNALS, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        if len(n) > 2_000_000:
            for k in [k for k in n if k[-1] < w]:
                del n[k]
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    p.add_argument("--kafka-topic", default="ad-beacons", help="your impression and dismiss beacon topic")
    p.add_argument("--user-field", default="user_id")
    p.add_argument("--event-field", default="event")
    p.add_argument("--impression-value", default="impression")
    p.add_argument("--dismiss-value", default="dismiss")
    p.add_argument("--campaign-field", default="campaign_id")
    p.add_argument("--advertiser-field", default="advertiser_id")
    p.add_argument("--window-s", type=float, default=86_400, help="cap window in seconds; a day by default")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
