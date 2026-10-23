"""Shadow stage changes from an existing Cloud Pub/Sub topic into Apache Iggy, read-only.

Use it if your publishing pipeline already publishes episode stage changes (uploaded, queued, transcoding,
transcoded, analysed, live) to a Pub/Sub topic. Spotify has not published how its publishing pipeline signals
stage changes, so the topic and the field names below are placeholders: map yours in to_msg(). If no such topic
exists, the upload API and the pipeline's workers write to the lane directly, one small write per stage change.

Reads through its own subscription ("iggy-shadow"). A Pub/Sub topic delivers every message to every
subscription, so the publishers and every subscriber you run today are untouched. Create it once:

  gcloud pubsub subscriptions create iggy-shadow --topic=<your-stage-change-topic> \
      --enable-message-ordering --message-retention-duration=1h

Then run status.py against Iggy and compare its queues and alerts with your dashboards.

The kit needs: the episode key (show plus content digest), the stage (0 received .. 4 live), the priority queue,
the time it was queued, and a per-episode event number n. Pub/Sub can redeliver, and without ordering keys it
can reorder, so publish with the episode as the ordering key, or take n from a per-episode counter your pipeline
already keeps; status.py drops anything at or below the last n it applied.
Needs: pip install google-cloud-pubsub apache-iggy==0.9.0, and Application Default Credentials with
roles/pubsub.subscriber on the subscription.
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage
from google.cloud import pubsub_v1

from common import DEFAULT_URL, NEW, STAGES, STATUS, STREAM, connect, ensure_topics, part


def to_msg(data: bytes, attrs: dict, publish_ns: int) -> tuple[str, bytes]:
    d = json.loads(data)
    k = d.get("episode") or attrs.get("episode") or f"{d['show_id']}:{d['content_hash']}"
    stage = d.get("stage", "received")
    out = {"e": k, "st": STAGES.index(stage) if isinstance(stage, str) else int(stage),
           "pr": d.get("priority", NEW), "q": int(d.get("queued_ns") or publish_ns),
           "n": int(d.get("n") or d.get("event_seq") or publish_ns // 1000),
           "k": "reupload" if d.get("reupload") else "stage", "dup": int(bool(d.get("reupload"))),
           "ns": publish_ns}                  # Pub/Sub publish time, so latency includes the hop
    return k, json.dumps(out, separators=(",", ":")).encode()


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy, STATUS)
    loop, inbox = asyncio.get_running_loop(), asyncio.Queue()

    def on_message(m):                        # runs on the Pub/Sub client's threads
        loop.call_soon_threadsafe(inbox.put_nowait, m)

    sub = pubsub_v1.SubscriberClient()
    path = sub.subscription_path(a.project, a.subscription)
    stream = sub.subscribe(path, callback=on_message,
                           flow_control=pubsub_v1.types.FlowControl(max_messages=a.max_outstanding))
    print(f"[bridge] reading {path} into Iggy {STREAM}/{STATUS}", flush=True)
    sent, mark = 0, time.monotonic()
    try:
        while True:
            got = [await inbox.get()]
            while not inbox.empty() and len(got) < 5000:
                got.append(inbox.get_nowait())
            batches = defaultdict(list)
            for m in got:
                k, body = to_msg(m.data, dict(m.attributes), int(m.publish_time.timestamp() * 1e9))
                batches[part(k, a.partitions)].append(SendMessage(body))
            # one batch per partition, same partition function as pipeline.py, sent at once
            await asyncio.gather(*(iggy.send_messages(STREAM, STATUS, Partitioning.partition_id(pid), ms)
                                   for pid, ms in batches.items()))
            for m in got:
                m.ack()                       # acked on the shadow subscription only, after Iggy has it
            sent += len(got)
            if time.monotonic() - mark >= 1:
                print(f"[bridge] {sent:,} stage changes/s copied", flush=True)
                sent, mark = 0, time.monotonic()
    finally:
        stream.cancel()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True, help="the GCP project that holds the subscription")
    p.add_argument("--subscription", default="iggy-shadow")
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--max-outstanding", type=int, default=20000, help="Pub/Sub flow control")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
