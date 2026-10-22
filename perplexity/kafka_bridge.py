"""Shadow your agent-action usage events from Kafka (or MSK, or any Kafka-protocol topic) into the Apache Iggy
agent-actions lane, read-only.

Runs as its own consumer group ("iggy-shadow"), so the services that emit usage events, the billing platform,
PostgreSQL and the Stripe sync are untouched. For each usage event it keeps a running credit total per
(runner, account) and publishes it with a sequence, keyed by account, the same shape agent_runners.py writes.
Point credit_ledger.py at Iggy (with your plans in common.plan()) and compare its pause decisions with the
ones your enforcement makes today.

Map your record in to_action(): the kit needs the account, the runner (or any producer ID) and the credits
charged for this action; a USD cost can be converted with --usd-field (100 credits = $1). The defaults read
JSON with fields account_id, runner and credits. If your usage events travel on Kinesis or Pub/Sub instead,
replace the consumer below with that client; the rest stays the same.
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

from common import ACTIONS, CREDITS_PER_USD, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg, part, period


def to_action(value: bytes, a):
    d = json.loads(value)
    cr = round(float(d[a.usd_field]) * CREDITS_PER_USD) if a.usd_field else int(d.get(a.credits_field, 0))
    return str(d[a.account_field]), str(d.get(a.runner_field, "unknown")), cr, str(d.get(a.kind_field, "action"))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, ACTIONS)
    producer = await iggy.producer(STREAM, ACTIONS, topic_partitions_count=PARTITIONS[ACTIONS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    seq, cum = defaultdict(int), defaultdict(int)
    async with producer:
        while True:
            p = period(a.period_s)
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    u, s, cr, kind = to_action(rec.value(), a)
                except (KeyError, ValueError):
                    continue                # not a usage record this mapping understands
                if cr <= 0:
                    continue                # free work (an Ask search): no credits
                seq[(s, u)] += 1
                cum[(s, u)] += cr
                groups[part(u, PARTITIONS[ACTIONS])].append(
                    SendMessage(msg(s=s, a=u, p=p, seq=seq[(s, u)], k=kind, cr=cum[(s, u)])))
            # One batch per partition, all sent at once, so an account's actions stay in order.
            if groups:
                await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                       for q, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers, host:port")
    ap.add_argument("--topic", default="agent-usage", help="your agent-action usage topic")
    ap.add_argument("--account-field", default="account_id")
    ap.add_argument("--runner-field", default="runner")
    ap.add_argument("--kind-field", default="kind")
    ap.add_argument("--credits-field", default="credits", help="credits charged for this action")
    ap.add_argument("--usd-field", default="", help="or a cost in USD, times 100")
    ap.add_argument("--period-s", type=float, default=30 * 86_400, help="month length in seconds; 30 days by default")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
