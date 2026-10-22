"""Shadow your usage messages into the Apache Iggy usage lane, read-only, from the Kill Bill plugin's Kafka topic.

Runs as its own consumer group ("iggy-shadow") on the topic your services already publish usage to for the
Kill Bill Kafka consumer plugin (stream.usagemetrics in the plugin's README), so the services, the plugin and
Kill Bill are untouched: a new consumer group gets its own copy of every message and moves no one else's offset.

Each message is the plugin's JSON (subscriptionExternalKey, trackingId, tenantId, unitUsageRecords). The bridge
adds the lane fields, keyed by subscription: a per-subscription sequence and the subscription's txnCount and
txnVolume for each recordDate so far. A trackingId it has already bridged (Kafka delivers at least once too) is
skipped, using a bounded window of recent trackingIds (--dedupe-window).

Unit names: the plugin's README uses txnCount and txnVolume; map yours with --count-unit / --volume-unit.
Other units are passed through to Kill Bill unchanged and left out of the lane's totals.
In a shadow run only the lane is timed; the clock starts when the bridge reads the message.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example (the plugin's README shows SSL with a keystore and truststore):
  -X security.protocol=SSL -X ssl.ca.location=... -X ssl.certificate.location=... -X ssl.key.location=...
"""
import argparse
import asyncio
import json
import time
from collections import OrderedDict, defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STREAM, USAGE, ensure_topics, part


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, USAGE)
    producer = await iggy.producer(STREAM, USAGE, topic_partitions_count=PARTITIONS[USAGE])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    seq = defaultdict(int)
    day_tot = {}                            # subscription -> (recordDate, txnCount, txnVolume) so far
    recent = OrderedDict()                  # trackingIds already bridged, oldest first
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    u = json.loads(rec.value())
                    sub, tid = str(u["subscriptionExternalKey"]), str(u["trackingId"])
                    units = u["unitUsageRecords"]
                except (KeyError, ValueError, TypeError):
                    continue                # not a usage message this mapping understands
                if tid in recent:
                    continue                # Kafka delivered it again: already bridged
                recent[tid] = None
                if len(recent) > a.dedupe_window:
                    recent.popitem(last=False)
                day, c, v = day_tot.get(sub, (None, 0, 0))
                for unit in units:
                    for r in unit.get("usageRecords", []):
                        d = str(r["recordDate"])[:10]
                        if d != day:
                            day, c, v = d, 0, 0     # a new day (or a record for another day) starts again
                        if unit.get("unitType") == a.count_unit:
                            c += int(r["amount"])
                        elif unit.get("unitType") == a.volume_unit:
                            v += int(r["amount"])
                day_tot[sub] = (day, c, v)
                seq[sub] += 1
                u.update(seq=seq[sub], cnt=c, vol=v, ns=time.time_ns())
                groups[part(sub, PARTITIONS[USAGE])].append(SendMessage(json.dumps(u, separators=(",", ":")).encode()))
            # One batch per partition, all sent at once, so a subscription's usage stays in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="your Kafka brokers, host:port")
    ap.add_argument("--topic", default="stream.usagemetrics", help="the topic the Kill Bill plugin consumes")
    ap.add_argument("--count-unit", default="txnCount")
    ap.add_argument("--volume-unit", default="txnVolume")
    ap.add_argument("--dedupe-window", type=int, default=2_000_000, help="recent trackingIds remembered")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
