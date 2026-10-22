"""Shadow your billing events from Kafka into the Apache Iggy charges lane, read-only.

Runs as its own consumer group ("iggy-shadow"), so the services that produce billing events, the wallet that
deducts today, invoicing and the partner portal are untouched. It reads the billing-event shape Gupshup
documents for inbound events (v2): app, payload.deductions (category, billable) and payload.references.gsId.
Each billable event is priced from --prices (USD per category; your rate card) and charged once per gsId,
then published as the running total for (source partition, app) with a sequence, keyed by app, the same shape
send_api.py writes. Point wallet.py at Iggy and compare its blocks with the 1003 rejections you see today.

The record has no fee amount, so the kit prices it; if your internal record carries the deducted amount,
read it in to_charge() instead. In a shadow run only the lane is timed; the clock starts when the bridge
reads the record. Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import OrderedDict, defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import CHARGES, DEFAULT_URL, GS_FEE, PARTITIONS, STREAM, ensure_topics, msg, part


def to_charge(value: bytes, prices: dict):
    d = json.loads(value)
    if d.get("type") != "billing-event":
        return None
    p = d["payload"]
    ded, ref = p["deductions"], p["references"]
    if str(ded.get("billable")).lower() != "true":
        return None                         # not billable (free tier, free entry point)
    cat = str(ded.get("category") or ded.get("type", "")).lower().replace(" ", "_")
    return str(d["app"]), str(ref["gsId"]), float(prices.get(cat, 0.0))


async def main(a):
    prices = json.loads(a.prices)
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, CHARGES)
    producer = await iggy.producer(STREAM, CHARGES, topic_partitions_count=PARTITIONS[CHARGES])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    seen = OrderedDict()                    # recent gsIds: an event delivered twice is charged once
    seq = defaultdict(int)
    tot = defaultdict(lambda: [0, 0.0, 0.0])   # (source, app) -> [messages, gs fee, wa fee]
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    c = to_charge(rec.value(), prices)
                except (KeyError, ValueError, TypeError):
                    continue                # not a record this mapping understands
                if c is None:
                    continue
                ap, gs_id, fee = c
                if gs_id in seen:
                    continue
                seen[gs_id] = None
                if len(seen) > 1_000_000:
                    seen.popitem(last=False)
                n = f"k{rec.partition()}"
                t = tot[(n, ap)]
                t[0] += 1
                t[1] += a.gs_fee
                t[2] += fee
                seq[(n, ap)] += 1
                groups[part(ap, PARTITIONS[CHARGES])].append(SendMessage(msg(
                    n=n, a=ap, seq=seq[(n, ap)], k="billing", m=t[0], gs=round(t[1], 6), wa=round(t[2], 6), cr=0)))
            # One batch per partition, all sent at once, so an app's charges stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="your Kafka brokers, host:port")
    ap.add_argument("--topic", default="billing-events", help="the topic that carries billing events")
    ap.add_argument("--prices", default='{"marketing": 0.0118, "utility": 0.0016, "authentication": 0.0016, '
                                         '"authentication_international": 0.028, "marketing_lite": 0.0118, "service": 0.004}',
                    help="Meta fee in USD per category (JSON); illustrative defaults, use your rate card")
    ap.add_argument("--gs-fee", type=float, default=GS_FEE, help="Gupshup fee in USD per billable message")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
