"""Shadow your fills from Kafka into the Apache Iggy order group lane, read-only.

Runs as its own consumer group ("iggy-shadow"), so the matching engines, the services that produce fills and
every other consumer are untouched. For each fill that belongs to an order group it keeps a running total of
contracts per (shard, order group) and publishes the resulting total with a sequence, keyed by order group,
the same shape engine.py writes. Point tracker.py at Iggy, with your groups' limits, and it prints which
groups a cross-shard order group would have triggered, and how many of those no single shard saw.

Map your record in to_fill(): the kit needs the order group ID, the exchange index (shard) the fill matched on,
the contracts filled and, optionally, the market ticker. The defaults read JSON with fields order_group_id,
exchange_index, count and ticker; fills without an order group are skipped.
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

from common import DEFAULT_URL, FILLS, PARTITIONS, STREAM, ensure_topics, msg, part


def to_fill(value: bytes, a):
    d = json.loads(value)
    g = d.get(a.group_field)
    if not g:
        return None                         # not in an order group
    return str(g), int(d.get(a.shard_field, 0)), int(float(d[a.count_field])), str(d.get(a.ticker_field, ""))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, FILLS)
    producer = await iggy.producer(STREAM, FILLS, topic_partitions_count=PARTITIONS[FILLS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": False}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    seq, cum = defaultdict(int), defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    f = to_fill(rec.value(), a)
                except (KeyError, ValueError):
                    continue                # not a fill this mapping understands
                if not f:
                    continue
                g, sh, c, mkt = f
                seq[(sh, g)] += 1
                cum[(sh, g)] += c
                groups[part(g, PARTITIONS[FILLS])].append(
                    SendMessage(msg(sh=sh, g=g, mkt=mkt, c=c, seq=seq[(sh, g)], cum=cum[(sh, g)])))
            # One batch per partition, all sent at once, so a group's fills stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="your Kafka brokers, host:port")
    ap.add_argument("--topic", default="fills", help="your fills topic")
    ap.add_argument("--group-field", default="order_group_id")
    ap.add_argument("--shard-field", default="exchange_index")
    ap.add_argument("--count-field", default="count", help="contracts filled")
    ap.add_argument("--ticker-field", default="ticker")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
