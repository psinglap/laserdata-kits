"""Shadow one of your existing status topics into its Apache Iggy topic, read-only.

Run one bridge per status type, each from that status's topic today into the matching Iggy
topic (status-sent, status-delivered, status-read). It reads through its own subscription or
consumer group, so the webhook endpoint, the providers and every reporting consumer you run
today are untouched.

Cloud Pub/Sub (a topic delivers every message to every subscription). Create one shadow
subscription per status topic, once:

  gcloud pubsub subscriptions create iggy-shadow-delivered --topic=<your-delivered-topic> \
      --message-retention-duration=1h
  python status_bridge.py --from pubsub --project <gcp-project> --subscription iggy-shadow-delivered --status delivered

Kafka (its own consumer group "iggy-shadow", offsets of other groups untouched):

  python status_bridge.py --from kafka --bootstrap <brokers> --topic <your-delivered-topic> --status delivered

Then point reporter.py at Iggy and compare its counts with your campaign report.

Map your webhook records in to_msg(): the kit needs m (the message ID the provider reports
on) and c (campaign). Providers retry and both sources can redeliver, so the reporter counts
each status once per message ID. Latency is measured from the source's publish time.
Needs: pip install apache-iggy==0.9.0 plus google-cloud-pubsub or confluent-kafka.
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, STATUSES, STREAM, connect, ensure_topic, part


def to_msg(data: bytes, attrs: dict, publish_ns: int, status: str) -> tuple[str, bytes]:
    d = json.loads(data)
    m = str(d.get("m") or d.get("message_id") or d.get("msg_id") or d.get("id") or attrs.get("message_id") or "none")
    c = str(d.get("c") or d.get("campaign_id") or d.get("campaign") or attrs.get("campaign_id") or "none")
    out = {"m": m, "c": c, "s": status, "ns": publish_ns}
    return m, json.dumps(out, separators=(",", ":")).encode()


def start_pubsub(a, loop, inbox):
    from google.cloud import pubsub_v1
    sub = pubsub_v1.SubscriberClient()
    path = sub.subscription_path(a.project, a.subscription)

    def on_message(m):                                   # runs on the Pub/Sub client's threads
        loop.call_soon_threadsafe(inbox.put_nowait, (m.data, dict(m.attributes),
                                                     int(m.publish_time.timestamp() * 1e9), m.ack))

    sub.subscribe(path, callback=on_message, flow_control=pubsub_v1.types.FlowControl(max_messages=a.max_outstanding))
    return path


def start_kafka(a, loop, inbox):
    import threading
    from confluent_kafka import Consumer
    k = Consumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
                  "enable.auto.commit": True, "auto.commit.interval.ms": 1000})
    k.subscribe([a.topic])

    def run():                                           # a thread of its own: poll() blocks
        while True:
            for r in k.consume(num_messages=1000, timeout=0.1):
                if r.error() is None:
                    loop.call_soon_threadsafe(inbox.put_nowait, (r.value(), {}, r.timestamp()[1] * 1_000_000, None))

    threading.Thread(target=run, daemon=True).start()
    return f"kafka {a.topic} (group iggy-shadow)"


async def main(a):
    topic, parts = STATUSES[a.status]
    iggy = await connect(a.url)
    await ensure_topic(iggy, STREAM, topic, parts)
    loop, inbox = asyncio.get_running_loop(), asyncio.Queue()
    src = start_pubsub(a, loop, inbox) if a.source == "pubsub" else start_kafka(a, loop, inbox)
    print(f"[bridge] reading {src} into Iggy {STREAM}/{topic}", flush=True)
    sent, mark = 0, time.monotonic()
    while True:
        got = [await inbox.get()]
        while not inbox.empty() and len(got) < 5000:
            got.append(inbox.get_nowait())
        batches = defaultdict(list)
        for data, attrs, ns, _ in got:
            m, body = to_msg(data, attrs, ns, a.status)
            batches[part(m, parts)].append(SendMessage(body))
        # one batch per partition, same partition function as webhooks.py, sent at once
        await asyncio.gather(*(iggy.send_messages(STREAM, topic, Partitioning.partition_id(pid), ms)
                               for pid, ms in batches.items()))
        for *_, ack in got:
            if ack:
                ack()                                    # acked on the shadow subscription only, after Iggy has it
        sent += len(got)
        if time.monotonic() - mark >= 1:
            print(f"[bridge] {sent:,} {a.status} webhooks/s copied", flush=True)
            sent, mark = 0, time.monotonic()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="source", choices=["pubsub", "kafka"], default="pubsub")
    p.add_argument("--status", choices=list(STATUSES), required=True)
    p.add_argument("--project", help="Pub/Sub: the GCP project that holds the subscription")
    p.add_argument("--subscription", help="Pub/Sub: the shadow subscription, e.g. iggy-shadow-delivered")
    p.add_argument("--max-outstanding", type=int, default=20000, help="Pub/Sub flow control")
    p.add_argument("--bootstrap", help="Kafka: bootstrap servers")
    p.add_argument("--topic", help="Kafka: the status topic to shadow")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
