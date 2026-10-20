"""Shadow an existing listing-change stream into Apache Iggy, read-only.

Today's source: an Azure Event Hub, read as its own consumer group ("iggy-shadow"), with no checkpoints written,
so producers and every existing consumer are untouched. After a move to a Kafka-compatible broker on AWS, the
same lane can shadow that topic instead (--source kafka, consumer group "iggy-shadow").

Point event_pages.py at Iggy and compare. Map your records in to_change(): the kit needs k ("event:listing"),
seq (per listing, increasing), qty (tickets left) and price.

Needs: pip install apache-iggy==0.9.0 azure-eventhub     (Event Hubs)
       pip install apache-iggy==0.9.0 confluent-kafka     (a Kafka-compatible broker)
The "iggy-shadow" consumer group must exist on the Event Hub first (portal, CLI or Terraform).
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, LISTINGS, PARTITIONS, STREAM, part_of


def to_change(body: bytes | str) -> tuple[str, bytes]:
    d = json.loads(body)                    # map your field names here
    d.setdefault("ns", time.time_ns())      # when the bridge saw it, if the record has no creation time in ns
    return d["k"], json.dumps(d, separators=(",", ":")).encode()


async def send(producer, records):
    batches = defaultdict(list)             # same partition function as the kit: one listing, one partition
    for k, payload in records:
        batches[part_of(k)].append(SendMessage(payload))
    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                           for pid, ms in batches.items()))


async def from_event_hub(a, producer):
    from azure.eventhub.aio import EventHubConsumerClient

    hub = EventHubConsumerClient.from_connection_string(a.connection_string, consumer_group="iggy-shadow",
                                                        eventhub_name=a.event_hub)

    async def on_batch(partition_context, events):  # no update_checkpoint: the bridge leaves no trace on the hub
        if events:
            await send(producer, [to_change(e.body_as_str()) for e in events])

    async with hub:
        await hub.receive_batch(on_event_batch=on_batch, starting_position="@latest", max_batch_size=1000,
                                max_wait_time=0.005)


async def from_kafka(a, producer):
    from confluent_kafka import Consumer

    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    while True:
        recs = [r for r in kafka.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            await send(producer, [to_change(r.value()) for r in recs])
        await asyncio.sleep(0)


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, LISTINGS, topic_partitions_count=PARTITIONS)
    async with producer:
        await (from_event_hub if a.source == "eventhub" else from_kafka)(a, producer)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=("eventhub", "kafka"), default="eventhub")
    p.add_argument("--connection-string", help="Event Hubs namespace connection string (listen rights are enough)")
    p.add_argument("--event-hub", help="the event hub that carries listing changes")
    p.add_argument("--bootstrap", help="Kafka-compatible bootstrap servers")
    p.add_argument("--topic", default="listing-changes", help="Kafka topic")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
