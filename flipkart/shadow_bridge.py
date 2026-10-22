"""Shadow your billable ad events into the Apache Iggy lane, read-only, from Cloud Pub/Sub or from Kafka.

Flipkart moved its ingestion to Cloud Pub/Sub, and still runs Kafka between services; which one carries the
ads telemetry today is yours to pick:
  --source pubsub  reads through its own subscription ("iggy-shadow"). A Pub/Sub topic delivers every message
                   to every subscription, so the collectors, the Flink speed layer and every other subscriber
                   are untouched. Create it once:
                     gcloud pubsub subscriptions create iggy-shadow --topic=<ads-events-topic> --message-retention-duration=1h
  --source kafka   reads as its own consumer group ("iggy-shadow"), so producers and every existing consumer
                   are untouched.

Map your record in to_event(): the kit needs the campaign, the impression ID as the collector sends it, the event
time in ms, the kind (view or click) and the cost in ₹ (or a CPM to divide by 1,000). Repeats and stale events
are passed on as they come: BCAP drops them (bcap.py), which is part of what the shadow run shows. Every 100 ms
the bridge writes a watermark mark to every partition. Then run bcap.py with --day-s 86400 --budgets-csv <your budgets> and
compare its spend and stops per campaign with BCAP's. In a shadow run only the lane is timed; the clock starts
when the bridge reads the record.
Needs: pip install apache-iggy==0.9.0 and google-cloud-pubsub (Application Default Credentials with
roles/pubsub.subscriber on the subscription) or confluent-kafka (client settings with -X).
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import BILLABLE, DEFAULT_URL, PARTITIONS, STREAM, day, ensure_topics, msg, part


def to_event(data: bytes, a) -> dict | None:
    d = json.loads(data)
    kind = str(d.get(a.kind_field, "view")).lower()
    if kind not in ("view", "impression", "click"):
        return None                         # not billable
    cost = float(d[a.cpm_field]) / 1000 if a.cpm_field and kind != "click" else float(d[a.cost_field])
    return dict(c=str(d[a.campaign_field]), id=str(d[a.id_field]), et=int(d[a.time_field]),
                kind="click" if kind == "click" else "view", cost=cost, srv=str(d.get("host", "collector")))


def pubsub_source(a, loop, inbox):
    from google.cloud import pubsub_v1
    sub = pubsub_v1.SubscriberClient()
    path = sub.subscription_path(a.project, a.subscription)

    def on_message(m):                      # runs on the Pub/Sub client's threads
        loop.call_soon_threadsafe(inbox.put_nowait, (m.data, m.ack))

    sub.subscribe(path, callback=on_message, flow_control=pubsub_v1.types.FlowControl(max_messages=a.max_outstanding))
    return path


def kafka_source(a, loop, inbox):
    from confluent_kafka import Consumer
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])

    def pump():                             # a thread: Kafka's poll blocks
        while True:
            for rec in kafka.consume(num_messages=1000, timeout=0.05):
                if not rec.error():
                    loop.call_soon_threadsafe(inbox.put_nowait, (rec.value(), None))

    loop.run_in_executor(None, pump)
    return a.kafka_topic


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, BILLABLE)
    producer = await iggy.producer(STREAM, BILLABLE, topic_partitions_count=PARTITIONS[BILLABLE])
    loop, inbox = asyncio.get_running_loop(), asyncio.Queue()
    src = (pubsub_source if a.source == "pubsub" else kafka_source)(a, loop, inbox)
    print(f"[bridge] reading {src} ({a.source}) into Iggy {STREAM}/{BILLABLE}", flush=True)
    sent, mark, next_wm = 0, time.monotonic(), 0.0
    async with producer:
        while True:
            try:
                got = [await asyncio.wait_for(inbox.get(), 0.1)]
            except asyncio.TimeoutError:
                got = []
            while not inbox.empty() and len(got) < 5000:
                got.append(inbox.get_nowait())
            batches = defaultdict(list)
            d = day(a.day_s)
            for data, _ in got:
                try:
                    e = to_event(data, a)
                except (KeyError, ValueError, TypeError):
                    continue                # not a record this mapping understands
                if e:
                    batches[part(e["c"], PARTITIONS[BILLABLE])].append(SendMessage(msg(day=d, **e)))
            if time.monotonic() >= next_wm:
                next_wm = time.monotonic() + 0.1
                for p in range(PARTITIONS[BILLABLE]):
                    batches[p].append(SendMessage(msg(wm=1)))
            # one batch per partition, same partition function as ad_servers.py, sent at once
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in batches.items()))
            for _, ack in got:
                if ack:
                    ack()                   # acked on the shadow subscription only, after Iggy has it
            sent += len(got)
            if time.monotonic() - mark >= 1:
                print(f"[bridge] {sent:,} events/s copied", flush=True)
                sent, mark = 0, time.monotonic()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=("pubsub", "kafka"), required=True)
    p.add_argument("--project", help="pubsub: the GCP project that holds the subscription")
    p.add_argument("--subscription", default="iggy-shadow")
    p.add_argument("--max-outstanding", type=int, default=20000, help="pubsub: flow control")
    p.add_argument("--bootstrap", help="kafka: a broker of your cluster")
    p.add_argument("--kafka-topic", default="ads-events")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="kafka: extra client setting")
    p.add_argument("--campaign-field", default="campaign_id")
    p.add_argument("--id-field", default="impression_id")
    p.add_argument("--time-field", default="event_time_ms")
    p.add_argument("--kind-field", default="event_type")
    p.add_argument("--cost-field", default="cost_inr", help="cost of this event in ₹")
    p.add_argument("--cpm-field", default="", help="or a CPM field for views, divided by 1,000")
    p.add_argument("--day-s", type=float, default=86_400, help="budget day in seconds; a real day by default")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
