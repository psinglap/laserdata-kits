"""Shadow the actions you send to Osprey from Cloud Pub/Sub into the Apache Iggy action lane, read-only.

Osprey can read its asynchronous input from Google Pub/Sub (InputStreamSource.PUBSUB) or Kafka. This
bridge reads the Pub/Sub topic through its own subscription ("iggy-shadow"). A Pub/Sub topic delivers
every message to every subscription, so the services that publish actions, the Osprey workers'
subscription and everything else you run are untouched. Create the subscription once:

  gcloud pubsub subscriptions create iggy-shadow --topic=<your Osprey actions topic> \
      --message-retention-duration=1h

Each action is written to the actions-input topic, keyed by user, with a per-user sequence number, the
shape actions.py writes. Then run rules_worker.py and effects_audit.py against it and compare verdicts
and timings with Osprey's. Delete the subscription when the shadow run ends.

Map your records in to_action(): the kit reads Osprey's JSON envelope ({"send_time", "data":
{"action_id", "action_name", "data": {...}}}) and needs the user ID, the action name, the account's
age in hours and whether a message has a link. Pub/Sub can redeliver and, without ordering keys,
reorder, so the per-user sequence is assigned here in the order the bridge reads.
If your actions go through Kafka instead, the same mapping works on a confluent-kafka consumer in its
own group (iggy-shadow) on osprey.actions_input.
Needs: pip install google-cloud-pubsub apache-iggy==0.9.0, and Application Default Credentials
with roles/pubsub.subscriber on the subscription.
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from google.cloud import pubsub_v1

from common import ACTIONS_TOPIC, DEFAULT_URL, STREAM, connect, ensure_topic, msg, part


def to_action(data: bytes, a):
    """Return (user id, action name, account age in hours, has link) from one Osprey action, or None."""
    env = json.loads(data)
    d = env.get("data", env)
    f = d.get("data", {})
    u = f.get(a.user_field) or f.get("user_id")
    if not u:
        return None
    text = str(f.get("content") or "")
    return (str(u), str(d.get("action_name", "unknown")), float(f.get(a.age_field, 1e6)),
            int(bool(f.get("has_link")) or "http" in text))


async def main(a):
    iggy = await connect(a.url)
    await ensure_topic(iggy, STREAM, ACTIONS_TOPIC, a.partitions)
    parts = (await iggy.get_topic(STREAM, ACTIONS_TOPIC)).partitions_count
    loop, inbox = asyncio.get_running_loop(), asyncio.Queue()

    def on_message(m):                        # runs on the Pub/Sub client's threads
        loop.call_soon_threadsafe(inbox.put_nowait, m)

    sub = pubsub_v1.SubscriberClient()
    path = sub.subscription_path(a.project, a.subscription)
    stream = sub.subscribe(path, callback=on_message,
                           flow_control=pubsub_v1.types.FlowControl(max_messages=a.max_outstanding))
    print(f"[bridge] reading {path} into Iggy {STREAM}/{ACTIONS_TOPIC} ({parts} partitions)", flush=True)
    seq = defaultdict(int)
    sent, mark = 0, time.monotonic()
    try:
        while True:
            got = [await inbox.get()]
            while not inbox.empty() and len(got) < 5000:
                got.append(inbox.get_nowait())
            batches = defaultdict(list)
            for m in got:
                act = to_action(m.data, a)
                if act is None:
                    continue
                u, name, age, link = act
                seq[u] += 1
                # the clock starts at the Pub/Sub publish time, so latency includes the hop
                batches[part(u, parts)].append(SendMessage(msg(
                    ns=int(m.publish_time.timestamp() * 1e9), u=u, a=name, seq=seq[u], age_h=age, link=link)))
            # one batch per partition, same partition function as actions.py, sent at once
            await asyncio.gather(*(iggy.send_messages(STREAM, ACTIONS_TOPIC, Partitioning.partition_id(pid), ms)
                                   for pid, ms in batches.items()))
            for m in got:
                m.ack()                       # acked on the shadow subscription only, after Iggy has it
            sent += len(got)
            if time.monotonic() - mark >= 1:
                print(f"[bridge] {sent:,} actions/s copied", flush=True)
                sent, mark = 0, time.monotonic()
    finally:
        stream.cancel()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True, help="the GCP project that holds the subscription")
    p.add_argument("--subscription", default="iggy-shadow")
    p.add_argument("--user-field", default="user_id", help="field in the action's data with the user ID")
    p.add_argument("--age-field", default="account_age_h", help="field with the account's age in hours")
    p.add_argument("--partitions", type=int, default=64, help="partitions for a new actions topic")
    p.add_argument("--max-outstanding", type=int, default=20000, help="Pub/Sub flow control")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
