"""Shadow your payment-attempt events from Kafka into the Apache Iggy outcome lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the router, every producer and every existing
consumer (the ClickHouse Kafka engine tables included) are untouched. For each attempt that reaches a
final status, it publishes the outcome with the release that served it, keyed by payment ID.
Point guard.py at Iggy and compare its decisions with your A/B framework's.

The defaults read Hyperswitch's payment-attempt events (topic hyperswitch-payment-attempt-events,
JSON rows with payment_id, attempt_id, status, payment_method, merchant_id, error_code and sign_flag).
Hyperswitch's events do not carry the server release, so tell the bridge where yours is: a Kafka
header (--release-header) or a field (--release-field). Name the release under test with --canary;
every other release counts as stable. Map other topics in to_outcome().
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, OUTCOMES, PARTITIONS, STREAM, SUCCESSES, ensure_topics, msg, part

# Final outcomes for the comparison: SUCCESSES, plus the failed statuses below (Hyperswitch AttemptStatus).
FINAL = SUCCESSES | {"authentication_failed", "authorization_failed", "router_declined", "failure", "capture_failed",
                     "expired"}


def to_outcome(rec, a):
    d = json.loads(rec.value())
    if int(d.get("sign_flag", 1)) != 1 or d.get("status") not in FINAL:
        return None                         # the retraction half of an update, or not final yet
    rel = None
    if a.release_header:
        rel = dict(rec.headers() or []).get(a.release_header)
        rel = rel.decode() if isinstance(rel, bytes) else rel
    if a.release_field:
        rel = d.get(a.release_field) or rel
    rel = str(rel or "unknown")
    return dict(pid=str(d["payment_id"]), aid=str(d.get("attempt_id", "")), rel=rel, canary=int(rel == a.canary),
                pm=str(d.get("payment_method") or "unknown").upper(), mid=str(d.get("merchant_id", "")),
                status=d["status"], code=d.get("error_code"))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, OUTCOMES)
    producer = await iggy.producer(STREAM, OUTCOMES, topic_partitions_count=PARTITIONS[OUTCOMES])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    attempts = {}                           # payment ID -> (attempt IDs seen, when), for the per-payment sequence
    last_sweep = time.monotonic()
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                o = to_outcome(rec, a)
                if not o:
                    continue
                seen, _ = attempts.get(o["pid"], ((), 0))
                if o["aid"] not in seen:
                    seen = seen + (o["aid"],)
                attempts[o["pid"]] = (seen, time.monotonic())
                o["seq"] = seen.index(o["aid"]) + 1
                groups[part(o["pid"], PARTITIONS[OUTCOMES])].append(SendMessage(msg(run="shadow", **o)))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in groups.items()))
            if time.monotonic() - last_sweep > 60:          # forget payments idle for ten minutes
                last_sweep = time.monotonic()
                for p in [p for p, (_, t) in attempts.items() if t < last_sweep - 600]:
                    del attempts[p]
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    p.add_argument("--kafka-topic", default="hyperswitch-payment-attempt-events", help="your payment-attempt events topic")
    p.add_argument("--release-header", default="x-release", help="Kafka header that names the serving release")
    p.add_argument("--release-field", default="", help="or a field in the record that names it")
    p.add_argument("--canary", required=True, help="the release under test, for example 2026.10.20.0")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
