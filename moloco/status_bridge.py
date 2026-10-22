"""Shadow your item status changes into the Apache Iggy item status lane, read-only, from Cloud Pub/Sub or
from Kafka.

Point it at wherever item status already flows: incremental catalog rows after import, Item Kill Switch
(UpdateItemStatusBulk) requests, or a retailer's stock feed. Each record needs an item ID, an availability
(in_stock, out_of_stock, preorder, backorder) and optionally a blocked reason, a stock count and an
updated_time (ISO 8601 or epoch ms). Map your field names with the --*-field flags, or edit to_record().

The bridge keeps Moloco Commerce Media's own rule: for one item, the newest updated_time wins, so an older
record that arrives late is dropped rather than undoing a newer one. Each change it writes carries the
resulting status and a per-item sequence, the same shape status.py writes, marked OUT when the item stops
being eligible for ads and BACK when it may be advertised again.

It never touches producers or existing readers:
  * Pub/Sub (default): reads through its own subscription ("iggy-shadow"); a topic delivers every message to
    every subscription. Create it once:
      gcloud pubsub subscriptions create iggy-shadow --topic=<item status topic> --message-retention-duration=1h
    Needs: pip install google-cloud-pubsub, and roles/pubsub.subscriber on the subscription.
  * Kafka (--kafka <bootstrap>): a consumer group of its own, "iggy-shadow", that only reads.
    Needs: pip install confluent-kafka.
Point decision_node.py at Iggy with --replay-s 0 and compare. In a shadow run the clock starts when the bridge
reads the record, so only the lane is timed.
"""
import argparse
import asyncio
import json
import threading
import time
from collections import defaultdict
from datetime import datetime

from apache_iggy import SendMessage

from common import BACK, DEFAULT_URL, NONE, OUT, PARTITIONS, STATUS, STREAM, connect, eligible, ensure_topic, msg, part


def stamp(v) -> float:
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)) or str(v).isdigit():
        return float(v) / 1000
    return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()


def to_record(data: bytes, attrs: dict, a):
    d = json.loads(data)
    get = lambda f: d.get(f, attrs.get(f))
    k = str(get(a.item_field))
    if a.seller_field and get(a.seller_field):
        k = f"{k}|{get(a.seller_field)}"          # MSPI: one status per item and seller
    avail = str(get(a.availability_field) or "in_stock").lower()
    stock = get(a.stock_field)
    stock = int(stock) if stock is not None else (1 if avail in ("in_stock", "preorder", "backorder") else 0)
    if avail == "out_of_stock":
        stock = 0
    return k, stock, str(get(a.blocked_field) or ""), stamp(get(a.time_field))


async def main(a):
    iggy = await connect(a.url)
    await ensure_topic(iggy, STATUS)
    writers = [await connect(a.url) for _ in range(PARTITIONS[STATUS])]
    loop, inbox = asyncio.get_running_loop(), asyncio.Queue()
    state = {}                                    # item -> [updated_time, stock, blocked, seq]
    pending, acks = defaultdict(list), defaultdict(list)
    cnt = dict(read=0, written=0, older=0)

    def on_record(k, stock, blocked, ut, done):
        s = state.get(k)
        if s is not None and ut and ut < s[0]:
            cnt["older"] += 1                     # an older updated_time than the one applied: the newest wins
            acks[0].append(done)
            return
        p = part(k)
        if s is None:
            s = state[k] = [ut, stock, blocked, 0]
            was = None
        else:
            was = eligible(s[1], s[2])
            if (stock > 0) == (s[1] > 0) and blocked == s[2] and stock == s[1]:
                s[0] = max(s[0], ut)
                acks[p].append(done)
                return                            # nothing changed
            s[0], s[1], s[2] = max(s[0], ut), stock, blocked
        now = eligible(stock, blocked)
        step = NONE if was is None or was == now else (BACK if now else OUT)
        s[3] += 1
        pending[p].append(SendMessage(msg(k, s[3], stock, blocked, step)))
        cnt["written"] += 1
        acks[p].append(done)

    async def write(p):
        while True:
            if pending[p] or acks[p]:
                batch, done = pending[p], acks[p]
                pending[p], acks[p] = [], []
                if batch:
                    await writers[p].send_messages(STREAM, STATUS, p, batch)
                for ack in done:
                    ack()                         # acked or committed on the shadow reader only, after Iggy has it
            await asyncio.sleep(a.tick_ms / 1000)

    async def consume():
        mark = time.monotonic()
        while True:
            data, attrs, done = await inbox.get()
            try:
                on_record(*to_record(data, attrs, a), done)
            except (KeyError, ValueError, TypeError):
                done()                            # not a record this mapping understands
            cnt["read"] += 1
            if time.monotonic() - mark >= 1:
                print(f"[bridge] {cnt['read']:,} records/s read  {cnt['written']:,} changes written  "
                      f"{cnt['older']:,} older than applied", flush=True)
                cnt.update(read=0, written=0, older=0)
                mark = time.monotonic()

    if a.kafka:
        from confluent_kafka import Consumer as KafkaConsumer

        kc = KafkaConsumer({"bootstrap.servers": a.kafka, "group.id": "iggy-shadow", "enable.auto.commit": False,
                            "auto.offset.reset": "latest"})
        kc.subscribe([a.topic])
        print(f"[bridge] Kafka {a.kafka} {a.topic} (group iggy-shadow) -> Iggy {STREAM}/{STATUS}", flush=True)

        def poll():                               # a thread of its own: confluent-kafka blocks
            while True:
                m = kc.poll(0.1)
                if m is None or m.error():
                    continue
                attrs = {h[0]: h[1].decode() for h in (m.headers() or [])}
                loop.call_soon_threadsafe(inbox.put_nowait, (m.value(), attrs,
                                          lambda m=m: kc.commit(message=m, asynchronous=True)))

        threading.Thread(target=poll, daemon=True).start()
        await asyncio.gather(consume(), *(write(p) for p in range(PARTITIONS[STATUS])))
    else:
        from google.cloud import pubsub_v1

        sub = pubsub_v1.SubscriberClient()
        path = sub.subscription_path(a.project, a.subscription)

        def on_message(m):                        # runs on the Pub/Sub client's threads
            loop.call_soon_threadsafe(inbox.put_nowait, (m.data, dict(m.attributes), m.ack))

        stream = sub.subscribe(path, callback=on_message,
                               flow_control=pubsub_v1.types.FlowControl(max_messages=a.max_outstanding))
        print(f"[bridge] {path} -> Iggy {STREAM}/{STATUS}", flush=True)
        try:
            await asyncio.gather(consume(), *(write(p) for p in range(PARTITIONS[STATUS])))
        finally:
            stream.cancel()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--project", help="Pub/Sub: the GCP project that holds the subscription")
    p.add_argument("--subscription", default="iggy-shadow")
    p.add_argument("--kafka", help="read Kafka at this bootstrap address instead of Pub/Sub")
    p.add_argument("--topic", help="Kafka: the item status topic")
    p.add_argument("--item-field", default="item_id")
    p.add_argument("--seller-field", default="", help="MSPI catalogs: the seller_id field, so status is per item and seller")
    p.add_argument("--availability-field", default="availability")
    p.add_argument("--blocked-field", default="blocked")
    p.add_argument("--stock-field", default="stock", help="optional; absent means 1 if available, 0 if not")
    p.add_argument("--time-field", default="updated_time")
    p.add_argument("--max-outstanding", type=int, default=20000, help="Pub/Sub flow control")
    p.add_argument("--tick-ms", type=float, default=2)
    p.add_argument("--url", default=DEFAULT_URL)
    args = p.parse_args()
    if not args.kafka and not args.project:
        p.error("give --project for Pub/Sub, or --kafka and --topic for Kafka")
    asyncio.run(main(args))
