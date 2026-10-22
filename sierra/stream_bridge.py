"""Shadow your conversation events into the Apache Iggy turns lane, read-only, from Kafka or Kinesis.

  kafka:   a plain Kafka consumer in its own group ("iggy-shadow"), so every producer and every existing
           consumer (streaming ETL, the lakehouse, Insights) is untouched.
  kinesis: a GetRecords reader per shard, starting at LATEST, with no checkpoints written anywhere. It shares
           the stream's read limits (5 calls and 2 MB per second per shard) with your other readers; on a busy
           stream, register an enhanced fan-out consumer for the trial instead, or run it on a mirrored stream.

For each record it publishes one turn, keyed by customer, with the customer's turn sequence, the same shape
agents.py writes. Point memory.py at Iggy and compare its memory updates and timings with yours.

Map your record in to_turn(): the kit needs the customer (or end-user) ID, the channel, and the fact the turn
adds to memory if your events carry one (otherwise put your extraction in memory.extract()). The defaults read
JSON with fields customer_id, channel, text and fact.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install apache-iggy==0.9.0 and confluent-kafka (kafka) or boto3 (kinesis)
  python stream_bridge.py kafka --bootstrap <brokers> --topic <conversation-events> -X security.protocol=SASL_SSL ...
  python stream_bridge.py kinesis --stream-name <conversation-events> --region <region>
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, STREAM, TURNS, ensure_topics, msg, part


def to_turn(value: bytes, a):
    d = json.loads(value)
    fact = d.get(a.fact_field)
    return (str(d[a.customer_field]), str(d.get(a.channel_field, "unknown")),
            str(d.get(a.text_field, ""))[: a.max_chars], str(fact) if fact else None)


def kafka_source(a):
    from confluent_kafka import Consumer
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    c = Consumer(conf)
    c.subscribe([a.topic])

    def poll():
        return [r.value() for r in c.consume(num_messages=1000, timeout=0.001) if not r.error()]
    return poll


def kinesis_source(a):
    import boto3
    k = boto3.client("kinesis", region_name=a.region)
    shards = [s["ShardId"] for s in k.list_shards(StreamName=a.stream_name)["Shards"]]
    it = {s: k.get_shard_iterator(StreamName=a.stream_name, ShardId=s, ShardIteratorType="LATEST")["ShardIterator"]
          for s in shards}
    last = defaultdict(float)

    def poll():
        out = []
        for s in shards:
            if it.get(s) is None or time.monotonic() - last[s] < 0.2:   # at most 5 calls per second per shard
                continue
            last[s] = time.monotonic()
            try:
                r = k.get_records(ShardIterator=it[s], Limit=1000)
            except k.exceptions.ProvisionedThroughputExceededException:
                continue                    # the shard's read limit is shared: back off, your readers come first
            it[s] = r.get("NextShardIterator")
            out += [rec["Data"] for rec in r["Records"]]
        return out
    return poll


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, TURNS)
    producer = await iggy.producer(STREAM, TURNS, topic_partitions_count=PARTITIONS[TURNS])
    poll = kafka_source(a) if a.source == "kafka" else kinesis_source(a)
    seq = defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for value in poll():
                try:
                    k, ch, text, fact = to_turn(value, a)
                except (KeyError, ValueError):
                    continue                # not a turn record this mapping understands
                seq[k] += 1
                groups[part(k, PARTITIONS[TURNS])].append(
                    SendMessage(msg(k=k, ch=ch, rt="bridge", seq=seq[k], fact=fact, text=text)))
            # One batch per partition, all sent at once, so a customer's turns stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0 if groups else 0.005)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="source", required=True)
    kf = sub.add_parser("kafka")
    kf.add_argument("--bootstrap", required=True)
    kf.add_argument("--topic", default="conversation-events")
    kf.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    kn = sub.add_parser("kinesis")
    kn.add_argument("--stream-name", default="conversation-events")
    kn.add_argument("--region", default=None)
    for s in (kf, kn):
        s.add_argument("--customer-field", default="customer_id")
        s.add_argument("--channel-field", default="channel")
        s.add_argument("--text-field", default="text")
        s.add_argument("--fact-field", default="fact")
        s.add_argument("--max-chars", type=int, default=4000, help="truncate the text the lane carries")
        s.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
