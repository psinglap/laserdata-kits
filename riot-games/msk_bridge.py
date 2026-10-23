"""Shadow your combat actions from Amazon MSK into the Apache Iggy combat-action lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): every producer, every existing consumer and every
pipeline on your MSK clusters are untouched. For each record it publishes one shot, keyed by match, with a
per-match sequence, the same shape game_servers.py writes. Point anticheat.py at Iggy and compare its terminations
and timings with your own models'.

Map your record in to_shot(): the kit needs the match, the player, whether the shot hit, and the features your
model reads (the defaults: reaction time and snap); the game server host routes the termination back. The
defaults read JSON with fields match_id, puuid, server, weapon, reaction_ms, snap_deg, hit and headshot.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=AWS_MSK_IAM ...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import ACTIONS, DEFAULT_URL, PARTITIONS, STREAM, ensure_topics, msg, part


def to_shot(value: bytes, a) -> dict:
    d = json.loads(value)
    return {"h": d.get(a.server_field, "unknown"), "m": str(d[a.match_field]), "p": str(d[a.player_field]),
            "w": str(d.get(a.weapon_field, "")), "rt": float(d[a.rt_field]), "snap": float(d[a.snap_field]),
            "hit": bool(d[a.hit_field]), "hs": bool(d.get(a.hs_field, False))}


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, ACTIONS)
    producer = await iggy.producer(STREAM, ACTIONS, topic_partitions_count=PARTITIONS[ACTIONS])
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
                    s = to_shot(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not a combat action this mapping understands
                seq[s["m"]] += 1
                groups[part(s["m"], PARTITIONS[ACTIONS])].append(SendMessage(msg(**s, seq=seq[s["m"]])))
            # One batch per partition, all sent at once, so a match's shots stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your MSK bootstrap servers")
    p.add_argument("--kafka-topic", default="combat-actions", help="the topic your combat actions are on")
    p.add_argument("--match-field", default="match_id")
    p.add_argument("--player-field", default="puuid")
    p.add_argument("--server-field", default="server")
    p.add_argument("--weapon-field", default="weapon")
    p.add_argument("--rt-field", default="reaction_ms")
    p.add_argument("--snap-field", default="snap_deg")
    p.add_argument("--hit-field", default="hit")
    p.add_argument("--hs-field", default="headshot")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
