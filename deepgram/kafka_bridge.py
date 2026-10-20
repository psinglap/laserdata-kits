"""Shadow your usage messages from inference into the Apache Iggy usage lane, read-only, from a Kafka-protocol topic.

Runs as its own consumer group ("iggy-shadow"), so the inference services that produce usage messages and the
pipeline that meters, rates and invoices them are untouched. For each usage message it keeps the stream's
billable audio so far and publishes the resulting total with a per-stream sequence, keyed by project, the same
shape engine.py writes. Point meter.py at Iggy and compare its per-project minutes with your usage breakdown.

If your usage messages ride SQS or Pub/Sub instead: receiving from an SQS queue removes the message from your
pipeline, so shadow from a second queue subscribed to the same SNS topic; on Pub/Sub, add a second subscription.
Either way the change to swap in is the read loop below; to_usage() stays the same.

Map your message in to_usage(): the kit needs the request (stream) ID, project, API key, model, the billable
audio in this message in seconds, and whether the stream has closed. The defaults read JSON with fields
request_id, project_id, accessor, model, duration and is_final; use --cumulative if your messages already carry
the stream's total instead of a chunk.
With chunks, a message your pipeline itself delivers twice reaches Iggy twice with new sequences; with
--cumulative (a running total per request) such a repeat adds nothing, so prefer it when your messages allow.
In a shadow run only the lane is timed; the clock starts when the bridge reads the message.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STREAM, USAGE, ensure_topics, msg, part


def to_usage(value: bytes, a):
    d = json.loads(value)
    return (str(d[a.id_field]), str(d[a.project_field]), str(d.get(a.accessor_field, "unknown")),
            str(d.get(a.model_field, "nova-3")), round(float(d[a.duration_field]) * 1000), bool(d.get(a.final_field)))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, USAGE)
    producer = await iggy.producer(STREAM, USAGE, topic_partitions_count=PARTITIONS[USAGE])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    seq, total = defaultdict(int), defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    sid, pj, key, model, ms, fin = to_usage(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not a usage message this mapping understands
                chunk = ms - total[sid] if a.cumulative else ms
                if chunk < 0:
                    continue                # an older total than one already sent
                seq[sid] += 1
                total[sid] += chunk
                groups[part(pj, PARTITIONS[USAGE])].append(SendMessage(msg(
                    s="bridge", id=sid, pj=pj, k=key, m=model, seq=seq[sid], ms=total[sid], d=chunk, fin=int(fin))))
                if fin:
                    del seq[sid], total[sid]
            # One batch per partition, all sent at once, so a project's usage stays in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="your Kafka-protocol brokers, host:port")
    ap.add_argument("--topic", default="usage", help="the topic your inference services publish usage messages to")
    ap.add_argument("--id-field", default="request_id")
    ap.add_argument("--project-field", default="project_id")
    ap.add_argument("--accessor-field", default="accessor")
    ap.add_argument("--model-field", default="model")
    ap.add_argument("--duration-field", default="duration", help="billable audio in seconds")
    ap.add_argument("--final-field", default="is_final", help="true on the stream's last usage message")
    ap.add_argument("--cumulative", action="store_true", help="duration is the stream's total so far, not a chunk")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
