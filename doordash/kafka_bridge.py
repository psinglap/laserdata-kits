"""Shadow your delivery events from Kafka into the Apache Iggy store feedback lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so producers, the Kafka REST proxy, Flink
jobs and every existing consumer are untouched. For each Dasher event at a store (assigned,
arrived, picked up) it keeps that store's feedback: Dashers heading there, Dashers waiting at the
counter and the last five waits, and writes the resulting feedback to the lane with a per-store
sequence, the same shape dashers.py writes. Point dispatch.py at Iggy and compare its view with
what your dispatch sees today.

Map your record in to_event(): the kit needs the store id, the event kind (one of assigned,
arrived, picked_up), the delivery id, and optionally the market's utilization in percent. The
defaults read JSON with fields store_id, event, delivery_id and util. Events in the Iguazu
envelope (Protobuf payload, envelope in the record headers) need your deserializer here instead.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict, deque

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, FEEDBACK, FEEDBACK_PARTITIONS, STREAM, msg, part

KINDS = {"assigned": "dasher.assigned", "arrived": "dasher.arrived", "picked_up": "order.picked_up"}


def to_event(value: bytes, a):
    d = json.loads(value)
    return str(d[a.store_field]), str(d[a.event_field]), str(d.get(a.delivery_field, "")), d.get(a.util_field)


class Feedback:
    __slots__ = ("seq", "enroute", "waiting", "waits", "util")

    def __init__(self):
        self.seq, self.enroute, self.waiting, self.waits, self.util = 0, set(), {}, deque(maxlen=5), 0


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, FEEDBACK, topic_partitions_count=FEEDBACK_PARTITIONS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    print(f"[bridge] {a.kafka_topic} on {a.bootstrap} -> Iggy {STREAM}/{FEEDBACK}", flush=True)
    stores = defaultdict(Feedback)

    def on_event(r, kind, delivery, util):
        s, now = stores[r], time.monotonic()
        if kind == "assigned":
            s.enroute.add(delivery)
        elif kind == "arrived":
            s.enroute.discard(delivery)
            s.waiting[delivery] = now
        elif kind == "picked_up":
            s.enroute.discard(delivery)
            t0 = s.waiting.pop(delivery, None)
            s.waits.append(round((now - t0) / 60, 1) if t0 else 0.0)
        else:
            return None
        if util is not None:
            s.util = int(util)
        s.seq += 1
        return msg(r=r, seq=s.seq, ev=KINDS[kind], order=delivery, enroute=len(s.enroute), waiting=len(s.waiting),
                   waits=list(s.waits), util=s.util, ns=time.time_ns())

    async with producer:
        while True:
            batches = defaultdict(list)          # one batch per partition, same store -> partition function
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    r, kind, delivery, util = to_event(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                    # not a Dasher event this mapping understands
                body = on_event(r, kind, delivery, util)
                if body:
                    batches[part(r, FEEDBACK_PARTITIONS)].append(SendMessage(body))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in batches.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    p.add_argument("--kafka-topic", required=True, help="the topic your Dasher and delivery events land on")
    p.add_argument("--store-field", default="store_id")
    p.add_argument("--event-field", default="event", help="values: assigned, arrived, picked_up")
    p.add_argument("--delivery-field", default="delivery_id")
    p.add_argument("--util-field", default="util", help="market utilization in percent, if your events carry it")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
