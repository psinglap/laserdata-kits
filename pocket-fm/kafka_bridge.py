"""Shadow your listening events from Kafka into the Autoplay lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow") on the topic your listening events land on: the
apps, every producer, every existing consumer, the playlist pipeline and Redis are untouched. For each
record it publishes one action, keyed by user, with a sequence that only grows (the read time in ns, plus
one per record), with the same partitioning as the kit. Point autoplay.py at Iggy and compare its next
show with the one your Autoplay API serves today.

Map your record in to_action(). Pocket FM's event schema is not public; the defaults accept
{"user_id", "show_id", "event", "session_id", "unlock_method"} and a few common spellings, and map event
names to play, episode_complete, unlock, skip and show_complete. In a shadow run only the lane is timed;
the clock starts when the bridge reads the record.
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

from common import ACTIONS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg, part

KIND = {"play": "play", "play_start": "play", "show_start": "play", "autoplay_start": "play",
        "episode_complete": "episode_complete", "episode_end": "episode_complete",
        "unlock": "unlock", "episode_unlock": "unlock", "coin_unlock": "unlock", "ad_unlock": "unlock",
        "skip": "skip", "autoplay_skip": "skip",
        "show_complete": "show_complete", "series_complete": "show_complete"}


def to_action(value: bytes):
    d = json.loads(value)
    user = str(d.get("user_id") or d.get("uid") or d["userId"])
    show = str(d.get("show_id") or d.get("series_id") or d["showId"])
    kind = KIND.get(str(d.get("event") or d.get("event_name") or d.get("type") or "").lower())
    sess = str(d.get("session_id") or d.get("sessionId") or user)
    via = d.get("unlock_method") or d.get("via")
    return user, sess, kind, show, via


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)       # one connection for the one producer
    await iggy.connect()
    await ensure_topics(iggy, ACTIONS)
    producer = await iggy.producer(STREAM, ACTIONS, topic_partitions_count=PARTITIONS[ACTIONS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    print(f"[bridge] reading {a.kafka_topic} as group iggy-shadow, from the latest offset, read-only", flush=True)
    async with producer:
        while True:
            groups = defaultdict(list)
            recs = await asyncio.to_thread(kafka.consume, 1000, 0.05)
            seq = time.time_ns()            # newer than any sequence before, even across bridge restarts
            for rec in recs:
                if rec.error():
                    continue
                try:
                    user, sess, kind, show, via = to_action(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a listening event this mapping understands
                if not kind:
                    continue                # an event Autoplay does not use
                seq += 1
                extra = {"via": str(via)} if via else {}
                groups[part(user, PARTITIONS[ACTIONS])].append(
                    SendMessage(msg(user=user, sess=sess, seq=seq, kind=kind, show=show, **extra)))
            # One batch per partition, all sent at once, so a listener's actions stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", default="listening-events", help="the topic your listening events land on")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
