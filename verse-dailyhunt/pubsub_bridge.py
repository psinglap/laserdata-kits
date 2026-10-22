"""Shadow your auction outcomes (or your floor changes) from Cloud Pub/Sub into the Apache Iggy floor lane,
read-only.

Reads through its own subscription ("iggy-shadow"). A Pub/Sub topic delivers every message to every
subscription, so the bid nodes, the services that publish and every subscriber you run today (the
BigQuery feed included) are untouched. Create the subscription once:

  gcloud pubsub subscriptions create iggy-shadow --topic=<your auction-outcome topic> --message-retention-duration=1h

Two modes:
  * --source outcomes (default): each record is one auction outcome (placement, and the clearing price in
    rupees CPM, or none for a 204 no-bid). The bridge keeps a moving clearing price per placement and a
    floor at --floor-share of it, and writes a change when the floor moves by --min-move or more; a move of
    --step-move or more is marked as a step. This is a stand-in for your yield logic, enough to compare.
  * --source floors: each record is already a floor your yield engine set (placement, floor). The bridge
    copies it, marking a step the same way.
Map your fields with --placement-field, --price-field and --floor-field, or edit to_record().
Each change carries the resulting floor and a per-placement sequence, the same shape floors.py writes.
Point bidnode.py at Iggy with --replay-s 0 and compare. In a shadow run the clock starts when the bridge
reads the record, so only the lane is timed.

If your auction outcomes ride Kafka instead, swap the subscriber for a confluent-kafka consumer group
named "iggy-shadow"; the rest stays.
Needs: pip install google-cloud-pubsub apache-iggy==0.9.0, and Application Default Credentials with
roles/pubsub.subscriber on the subscription.
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import SendMessage
from google.cloud import pubsub_v1

from common import DEFAULT_URL, FLOORS, PARTITIONS, STREAM, connect, ensure_topic, msg, part


def to_record(data: bytes, attrs: dict, a):
    d = json.loads(data)
    k = str(d.get(a.placement_field) or attrs.get(a.placement_field))
    v = d.get(a.floor_field if a.source == "floors" else a.price_field)
    return k, (float(v) if v is not None else None)


async def main(a):
    iggy = await connect(a.url)
    await ensure_topic(iggy, FLOORS)
    writers = [await connect(a.url) for _ in range(PARTITIONS[FLOORS])]
    loop, inbox = asyncio.get_running_loop(), asyncio.Queue()

    def on_message(m):                        # runs on the Pub/Sub client's threads
        loop.call_soon_threadsafe(inbox.put_nowait, m)

    sub = pubsub_v1.SubscriberClient()
    path = sub.subscription_path(a.project, a.subscription)
    stream = sub.subscribe(path, callback=on_message,
                           flow_control=pubsub_v1.types.FlowControl(max_messages=a.max_outstanding))
    print(f"[bridge] {path} ({a.source}) -> Iggy {STREAM}/{FLOORS}", flush=True)
    state = {}                                # placement -> [moving clearing price, floor last written, seq]
    pending, acks = defaultdict(list), defaultdict(list)
    alpha = 1 / a.window

    def on_value(k, v, m):
        s = state.get(k)
        if a.source == "outcomes":
            if v is None:                     # a 204 no-bid: counts as demand below the floor
                v = (s[0] * 0.9) if s else None
                if v is None:
                    acks[0].append(m)
                    return
            if s is None:
                s = state[k] = [v, -1.0, 0]
            s[0] += alpha * (v - s[0])
            floor = s[0] * a.floor_share
        else:
            if v is None:
                acks[0].append(m)
                return
            if s is None:
                s = state[k] = [v, -1.0, 0]
            floor = v
        moved = abs(floor - s[1]) / s[1] if s[1] > 0 else 1.0
        p = part(k)
        if moved >= a.min_move:
            step = 0 if s[1] <= 0 or moved < a.step_move else (1 if floor > s[1] else -1)
            s[1] = floor
            s[2] += 1
            pending[p].append(SendMessage(msg(k, s[2], floor, step)))
        acks[p].append(m)

    async def write(p):
        while True:
            if pending[p] or acks[p]:
                batch, done = pending[p], acks[p]
                pending[p], acks[p] = [], []
                if batch:
                    await writers[p].send_messages(STREAM, FLOORS, p, batch)
                for m in done:
                    m.ack()                   # acked on the shadow subscription only, after Iggy has it
            await asyncio.sleep(a.tick_ms / 1000)

    async def consume():
        n, mark = 0, time.monotonic()
        while True:
            m = await inbox.get()
            try:
                on_value(*to_record(m.data, dict(m.attributes), a), m)
            except (KeyError, ValueError, TypeError):
                m.ack()                       # not a record this mapping understands
            n += 1
            if time.monotonic() - mark >= 1:
                print(f"[bridge] {n:,} records/s read", flush=True)
                n, mark = 0, time.monotonic()

    try:
        await asyncio.gather(consume(), *(write(p) for p in range(PARTITIONS[FLOORS])))
    finally:
        stream.cancel()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True, help="the GCP project that holds the subscription")
    p.add_argument("--subscription", default="iggy-shadow")
    p.add_argument("--source", choices=("outcomes", "floors"), default="outcomes")
    p.add_argument("--placement-field", default="placement")
    p.add_argument("--price-field", default="clear_cpm_rs", help="clearing price; null for a 204 no-bid")
    p.add_argument("--floor-field", default="floor_cpm_rs")
    p.add_argument("--floor-share", type=float, default=0.7, help="outcomes mode: floor = this x moving clearing price")
    p.add_argument("--window", type=float, default=200, help="outcomes mode: auctions in the moving clearing price")
    p.add_argument("--min-move", type=float, default=0.03, help="write a change when the floor moves this much (fraction)")
    p.add_argument("--step-move", type=float, default=0.25, help="a move this large is marked as a step")
    p.add_argument("--max-outstanding", type=int, default=20000, help="Pub/Sub flow control")
    p.add_argument("--tick-ms", type=float, default=2)
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
