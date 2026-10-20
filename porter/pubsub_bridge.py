"""Shadow your existing Cloud Pub/Sub topic of driver pings into Apache Iggy, read-only.

Reads through its own subscription ("iggy-shadow"). A Pub/Sub topic delivers every message to
every subscription, so the driver app, the services that publish pings and every subscriber you
run today (the PostGIS writers included) are untouched. Create the subscription once:

  gcloud pubsub subscriptions create iggy-shadow --topic=<your-ping-topic> --message-retention-duration=1h

Then point trips.py at Iggy and compare.

Map your records in to_msg(): the kit needs k (driver ID), lat, lon, and order if the ping
carries one. Pub/Sub can redeliver, and without ordering keys it can reorder, so seq is taken
from the ping's own device timestamp (milliseconds); trips.py keeps the newest position per
driver and drops anything older.
Needs: pip install google-cloud-pubsub apache-iggy==0.9.0, and Application Default Credentials
with roles/pubsub.subscriber on the subscription.
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from google.cloud import pubsub_v1

from common import DEFAULT_URL, PINGS, STREAM, part


def to_msg(data: bytes, attrs: dict, publish_ns: int) -> tuple[str, bytes]:
    d = json.loads(data)
    k = d.get("k") or d.get("driver_id") or d.get("partner_id") or attrs.get("driver_id") or "none"
    ts_ms = d.get("ts_ms") or d.get("timestamp_ms") or publish_ns // 1_000_000
    out = {"k": k, "seq": d.get("seq") or int(ts_ms), "lat": d["lat"], "lon": d["lon"],
           "order": d.get("order") or d.get("order_id"), "v": d.get("vehicle"),
           "ns": publish_ns}                  # Pub/Sub publish time, so latency includes the hop
    return k, json.dumps(out, separators=(",", ":")).encode()


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, a.iggy_topic, topic_partitions_count=a.partitions)
    loop, inbox = asyncio.get_running_loop(), asyncio.Queue()

    def on_message(m):                        # runs on the Pub/Sub client's threads
        loop.call_soon_threadsafe(inbox.put_nowait, m)

    sub = pubsub_v1.SubscriberClient()
    path = sub.subscription_path(a.project, a.subscription)
    stream = sub.subscribe(path, callback=on_message,
                           flow_control=pubsub_v1.types.FlowControl(max_messages=a.max_outstanding))
    print(f"[bridge] reading {path} into Iggy {STREAM}/{a.iggy_topic}", flush=True)
    sent, mark = 0, time.monotonic()
    async with producer:
        try:
            while True:
                got = [await inbox.get()]
                while not inbox.empty() and len(got) < 5000:
                    got.append(inbox.get_nowait())
                batches = defaultdict(list)
                for m in got:
                    k, body = to_msg(m.data, dict(m.attributes), int(m.publish_time.timestamp() * 1e9))
                    batches[part(k, a.partitions)].append(SendMessage(body))
                # one batch per partition, same partition function as drivers.py, sent at once
                await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in batches.items()))
                for m in got:
                    m.ack()                   # acked on the shadow subscription only, after Iggy has it
                sent += len(got)
                if time.monotonic() - mark >= 1:
                    print(f"[bridge] {sent:,} pings/s copied", flush=True)
                    sent, mark = 0, time.monotonic()
        finally:
            stream.cancel()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True, help="the GCP project that holds the subscription")
    p.add_argument("--subscription", default="iggy-shadow")
    p.add_argument("--iggy-topic", default=PINGS)
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--max-outstanding", type=int, default=20000, help="Pub/Sub flow control")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
