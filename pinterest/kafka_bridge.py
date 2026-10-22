"""Shadow your ad insertion events from Kafka into the Apache Iggy in-flight spend lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow") on the topic your ads serving writes insertion events
to (the input topic of your in-flight spend aggregator), so ads serving, the aggregator, the spend system and
every other consumer are untouched. For each insertion it computes in-flight spend (a field you already
carry, or price x impression_rate x action_rate), drops a repeat of an insertion ID it has already seen,
keeps a running total per (ad group, budget day) and publishes the resulting total with a sequence, keyed by
ad group, the same shape ad_server.py writes. Start ad_server.py processes with --rate 0 to read the lane
only, and compare their totals with your aggregator's output topic.

Map your record in to_insertion(): the kit needs the ad group, the insertion ID and either the in-flight spend
or its three factors. The defaults read JSON with fields adgroup_id, insertion_id, inflight_spend (dollars),
or price, impression_rate and action_rate. In a shadow run only the lane is timed: the clock starts when the
bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SSL -X ssl.ca.location=...
"""
import argparse
import asyncio
import json
from collections import OrderedDict, defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, SPEND, STREAM, budget_day, connect, ensure_topics, msg, part


def to_insertion(value: bytes, a):
    d = json.loads(value)
    if a.spend_field in d:
        spend = float(d[a.spend_field])
    else:
        spend = float(d[a.price_field]) * float(d.get(a.imp_rate_field, 1)) * float(d.get(a.action_rate_field, 1))
    return str(d[a.adgroup_field]), str(d[a.id_field]), int(round(spend * 1e6))   # micro-dollars


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy, SPEND)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq, n, seen = defaultdict(int), defaultdict(int), OrderedDict()
    P = PARTITIONS[SPEND]
    while True:
        day = budget_day(a.day_s)
        batches = defaultdict(list)
        for rec in kafka.consume(num_messages=1000, timeout=0.001):
            if rec.error():
                continue
            try:
                g, iid, micros = to_insertion(rec.value(), a)
            except (KeyError, ValueError, TypeError):
                continue                    # not a record this mapping understands
            if iid in seen:
                continue                    # a repeat of an insertion already published: count it once
            seen[iid] = None
            if len(seen) > a.dedup_ids:
                seen.popitem(last=False)
            seq[g] += 1
            n[(g, day)] += micros
            batches[part(g, P)].append(SendMessage(msg(s="bridge", g=g, d=day, seq=seq[g], n=n[(g, day)])))
        # One batch per partition, all sent at once, so an ad group's spend stays in order.
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, SPEND, Partitioning.partition_id(q), ms)
                                   for q, ms in batches.items()))
        if len(n) > 2_000_000:
            for k in [k for k in n if k[-1] < day]:
                del n[k]
        await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    ap.add_argument("--kafka-topic", default="ads-insertions", help="your ad insertion events topic")
    ap.add_argument("--adgroup-field", default="adgroup_id")
    ap.add_argument("--id-field", default="insertion_id")
    ap.add_argument("--spend-field", default="inflight_spend", help="in-flight spend in dollars, if you carry it")
    ap.add_argument("--price-field", default="price")
    ap.add_argument("--imp-rate-field", default="impression_rate")
    ap.add_argument("--action-rate-field", default="action_rate")
    ap.add_argument("--dedup-ids", type=int, default=5_000_000, help="recent insertion IDs remembered for de-duplication")
    ap.add_argument("--day-s", type=float, default=86_400, help="budget day in seconds; a day by default")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
