"""Shadow your hotel search events from Kafka into the last-searched lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow") on your B2C and B2B clickstream topics: the apps, every
producer, the Spark Real-Time Mode job, Aerospike, the Redis UI cache and every existing consumer are untouched.
For each hotel search or hotel view it publishes one event, keyed by user, with a sequence that only grows (the
read time in ns, plus one per record), with the same partitioning as the kit. Point recents.py at Iggy and
compare its lists with the "last-searched" list your app serves today.

Map your record in to_search(). MakeMyTrip's event schema is not public, so the defaults are guesses at common
field names (user_id / uuid, hotel_id, city, checkin, event); records without a hotel are skipped. Pass each
topic with its segment, for example --kafka-topic b2c-clickstream=b2c --kafka-topic b2b-clickstream=b2b.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
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

from common import DEFAULT_URL, PARTITIONS, SEARCHES, STREAM, ensure_topics, msg, part


def to_search(value: bytes):
    d = json.loads(value)
    user = str(d.get("user_id") or d.get("uuid") or d.get("userId") or d["deviceId"])
    h = str(d.get("hotel_id") or d.get("hotelId") or d["hotel"])
    city = str(d.get("city") or d.get("city_code") or d.get("cityCode") or "")
    checkin = str(d.get("checkin") or d.get("checkIn") or "")
    kind = str(d.get("event") or d.get("event_type") or "hotel_search")
    return user, h, city, checkin, kind


async def main(a):
    seg_of = dict(t.split("=", 1) if "=" in t else (t, "b2c") for t in a.kafka_topic)
    iggy = IggyClient.from_connection_string(a.url)       # one connection for the one producer
    await iggy.connect()
    await ensure_topics(iggy, SEARCHES)
    producer = await iggy.producer(STREAM, SEARCHES, topic_partitions_count=PARTITIONS[SEARCHES])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe(list(seg_of))
    print(f"[bridge] reading {', '.join(seg_of)} as group iggy-shadow, from the latest offset, read-only", flush=True)
    async with producer:
        while True:
            groups = defaultdict(list)
            recs = await asyncio.to_thread(kafka.consume, 1000, 0.05)
            seq = time.time_ns()            # newer than any sequence before, even across bridge restarts
            for rec in recs:
                if rec.error():
                    continue
                try:
                    user, h, city, checkin, kind = to_search(rec.value())
                except (KeyError, ValueError, TypeError):
                    continue                # not a hotel search this mapping understands
                seq += 1
                groups[part(user, PARTITIONS[SEARCHES])].append(SendMessage(msg(
                    user=user, seg=seg_of.get(rec.topic(), "b2c"), seq=seq, kind=kind, hotel=h, city=city,
                    checkin=checkin)))
            # One batch per partition, all sent at once, so a traveller's searches stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", action="append", required=True, metavar="topic[=b2c|b2b]",
                   help="a clickstream topic and its segment; repeat for each")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
