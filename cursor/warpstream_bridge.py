"""Shadow your raw usage events from WarpStream (or any Kafka-protocol topic) into the Apache Iggy usage lane, read-only.

Runs as its own consumer group ("iggy-shadow") through the Kafka protocol WarpStream speaks, so the services
that produce usage events, the metering pipeline, the ledger and the Stripe sync are untouched. For each
usage event it keeps a running paid usage per (API server, user, period) and publishes the resulting total
with a sequence, keyed by user, the same shape api_servers.py writes. Point limits.py at Iggy and compare
its stop decisions with the ones your enforcement makes today.

Map your record in to_usage(): the kit needs the user, the server (or any producer ID), the charged cost of
this request in USD, and optionally the event time. The defaults read JSON with fields user_id, host and
cost_usd; a cost in cents can be converted with --cents-field.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STREAM, USAGE, ensure_topics, msg, part, period


def to_usage(value: bytes, a):
    d = json.loads(value)
    cost = float(d[a.cents_field]) / 100 if a.cents_field else float(d.get(a.cost_field, 0))
    return str(d[a.user_field]), str(d.get(a.server_field, "unknown")), cost


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, USAGE)
    producer = await iggy.producer(STREAM, USAGE, topic_partitions_count=PARTITIONS[USAGE])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    seq, cum = defaultdict(int), defaultdict(float)
    async with producer:
        while True:
            p = period(a.period_s)
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    u, s, cost = to_usage(rec.value(), a)
                except (KeyError, ValueError):
                    continue                # not a usage record this mapping understands
                if cost <= 0:
                    continue                # included usage: not counted toward the spend limit
                seq[(s, u)] += 1
                cum[(p, s, u)] += cost
                groups[part(u, PARTITIONS[USAGE])].append(
                    SendMessage(msg(s=s, u=u, p=p, seq=seq[(s, u)], usd=round(cum[(p, s, u)], 6))))
            # One batch per partition, all sent at once, so a user's usage stays in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))
            for k in [k for k in cum if k[0] < p - 1] if len(cum) > 1_000_000 else []:
                del cum[k]
            await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="your WarpStream agent (Kafka protocol), host:port")
    ap.add_argument("--topic", default="usage-events", help="your raw usage-events topic")
    ap.add_argument("--user-field", default="user_id")
    ap.add_argument("--server-field", default="host")
    ap.add_argument("--cost-field", default="cost_usd", help="charged cost of this request in USD")
    ap.add_argument("--cents-field", default="", help="or a cost in cents, divided by 100")
    ap.add_argument("--period-s", type=float, default=30 * 86_400, help="billing period in seconds; 30 days by default")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
