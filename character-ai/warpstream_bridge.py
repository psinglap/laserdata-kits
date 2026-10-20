"""Shadow your chat turns from WarpStream into the Apache Iggy turns lane, read-only.

WarpStream speaks the Kafka protocol, so this is a plain Kafka consumer in its own group ("iggy-shadow"):
the tokenizer, every producer and every existing pipeline (warehouse, A/B tests, feature store) are
untouched. For each record it publishes one turn, keyed by conversation, with the conversation's turn
sequence, the same shape chat_servers.py writes. Point classifiers.py at Iggy and compare its verdicts
and timings with your classifiers'.

Map your record in to_turn(): the kit needs the conversation ID and the turn text (the user's message and
the reply; not the whole assembled prompt), and optionally the serving host. The defaults read JSON with
fields conversation_id, text and host.
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

from common import DEFAULT_URL, PARTITIONS, STREAM, TURNS, ensure_topics, msg, part


def to_turn(value: bytes, a):
    d = json.loads(value)
    return str(d[a.conversation_field]), str(d[a.text_field])[: a.max_chars], str(d.get(a.host_field, "unknown"))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, TURNS)
    producer = await iggy.producer(STREAM, TURNS, topic_partitions_count=PARTITIONS[TURNS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq = defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    c, text, host = to_turn(rec.value(), a)
                except (KeyError, ValueError):
                    continue                # not a turn record this mapping understands
                seq[c] += 1
                groups[part(c, PARTITIONS[TURNS])].append(SendMessage(msg(c=c, s=host, seq=seq[c], text=text)))
            # One batch per partition, all sent at once, so a conversation's turns stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your WarpStream Agents' Kafka endpoint")
    p.add_argument("--kafka-topic", default="chat-turns", help="the topic your turns or prompts are captured on")
    p.add_argument("--conversation-field", default="conversation_id")
    p.add_argument("--text-field", default="text")
    p.add_argument("--host-field", default="host")
    p.add_argument("--max-chars", type=int, default=4000, help="truncate the text the lane carries")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
