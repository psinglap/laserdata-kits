"""Shadow your cart and checkout events from Kafka into the buyer signal lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow") on the topics your cart, checkout and order events land
on (the Monorail or CDC topics the Inbox pipeline reads): Monorail, every producer, every existing consumer,
the Dataflow and Flink jobs are untouched. For each record it publishes one signal, keyed by buyer, carrying the
buyer's resulting state (status, item quantity, subtotal, order number) and a sequence taken from the event's
own time, so an event that arrives after a newer one for the same buyer is dropped by the conversation, as the
Inbox pipeline drops outdated events by comparing timestamps. Point inbox.py at Iggy and compare its view
with the one Inbox shows today.

Map your record in to_signal(). Shopify's internal event schemas are not public, so the defaults read the
public webhook payloads for the same events: carts/update (token, line_items with quantity and price),
checkouts/create (cart_token, subtotal_price) and orders/create (cart_token, order_number, subtotal_price),
keyed by a buyer field if your events have one, else by the cart token. Cart adds and removes are inferred
by comparing quantities with the buyer's previous cart, as the Inbox pipeline does. In a shadow run only the
lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import CART, CHECKOUT, DEFAULT_URL, ORDERED, PARTITIONS, SIGNALS, STREAM, ensure_topics, msg, part


def cents(x) -> int:
    return int(round(float(x or 0) * 100))


def event_ms(d: dict) -> int:
    t = d.get("updated_at") or d.get("created_at") or d.get("timestamp")
    if isinstance(t, (int, float)):
        return int(t if t > 1e11 else t * 1000)
    if t:
        return int(datetime.fromisoformat(str(t).replace("Z", "+00:00")).timestamp() * 1000)
    return time.time_ns() // 1_000_000


def to_signal(d: dict):
    """Return (buyer, event ms, status, {item: qty}, subtotal cents, order number) for one record, or None."""
    buyer = str(d.get("buyer") or d.get("buyer_id") or d.get("customer_id") or (d.get("customer") or {}).get("id")
                or d.get("cart_token") or d.get("token") or "")
    if not buyer:
        return None
    lines = {str(li.get("variant_id") or li.get("id") or li.get("title")): int(li.get("quantity", 0))
             for li in d.get("line_items", [])}
    if d.get("order_number") or d.get("financial_status"):
        status, order = ORDERED, f"#{d.get('order_number') or d.get('id')}"
    elif "abandoned_checkout_url" in d or d.get("checkout_token"):
        status, order = CHECKOUT, ""
    else:
        status, order = CART, ""
    subtotal = cents(d.get("subtotal_price") or d.get("total_price") or
                     sum(float(li.get("price", 0)) * int(li.get("quantity", 0)) for li in d.get("line_items", [])))
    return buyer, event_ms(d), status, lines, subtotal, order


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)       # one connection for the one producer
    await iggy.connect()
    await ensure_topics(iggy, SIGNALS)
    producer = await iggy.producer(STREAM, SIGNALS, topic_partitions_count=PARTITIONS[SIGNALS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe(a.kafka_topic)
    print(f"[bridge] reading {', '.join(a.kafka_topic)} as group iggy-shadow, from the latest offset, read-only",
          flush=True)
    last = {}                               # buyer -> (event ms, tie, {item: qty}): sequence and add/remove inference
    async with producer:
        while True:
            groups = defaultdict(list)
            recs = await asyncio.to_thread(kafka.consume, 1000, 0.05)
            for rec in recs:
                if rec.error():
                    continue
                try:
                    s = to_signal(json.loads(rec.value()))
                except (KeyError, ValueError, TypeError, AttributeError):
                    continue                # not a cart, checkout or order event this mapping understands
                if not s:
                    continue
                buyer, ms, status, lines, subtotal, order = s
                prev_ms, tie, prev_lines = last.get(buyer, (0, 0, {}))
                if status == CHECKOUT and not lines:
                    lines = prev_lines      # a checkout record without line items keeps the cart it came from
                tie = tie + 1 if ms == prev_ms else 0
                seq = ms * 1_000_000 + tie  # the event's own time: an older event gets a smaller sequence
                if status == ORDERED:
                    kind = "order_completed"
                elif status == CHECKOUT:
                    kind = "checkout_started"
                else:
                    kind = "cart_add" if sum(lines.values()) >= sum(prev_lines.values()) else "cart_remove"
                if ms >= prev_ms:
                    last[buyer] = (ms, tie, {} if status == ORDERED else lines)
                groups[part(buyer, PARTITIONS[SIGNALS])].append(SendMessage(msg(
                    buyer=buyer, seq=seq, kind=kind, product="", status=status, items=sum(lines.values()),
                    subtotal=subtotal, order=order)))
            if len(last) > a.max_buyers:
                last.clear()                # bounded memory: worst case, one add/remove label is guessed
            # One batch per partition, all sent at once, so a buyer's signals stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap servers")
    p.add_argument("--kafka-topic", action="append", required=True,
                   help="a topic your cart, checkout or order events land on (repeat for each)")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--max-buyers", type=int, default=2_000_000, help="buyers whose last cart the bridge remembers")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
