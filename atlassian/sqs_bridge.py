"""Shadow your Forge usage events from a StreamHub subscription into the Apache Iggy usage lane, read-only.

StreamHub fans events out to subscribers over SQS, SNS or Kinesis; consumers don't read its Kafka directly.
Declare one more subscription for the Forge usage events, delivered to an SQS queue of your own, and run this
bridge on that queue. It reads and deletes only from its own queue, so the Forge services, StreamHub, UTS,
Plato and Commerce are untouched.

For each usage event (a counter delta with its usageUniqueId) it drops ids it has already seen, keeps a
running total per (source, app, cycle) and publishes the resulting total with a sequence, keyed by app, the
same shape runtime.py writes. Point tracker.py at Iggy with --cycle-s 0 --allowance-gbs 200000 and compare
its crossings with the usage alert emails sent today.

Map your record in to_usage(): the kit needs the app, a source (any producer or host ID), the usageUniqueId
and the GB-seconds of this event. SNS envelopes ({"Message": "..."}) are unwrapped.
In a shadow run only the lane is timed; the clock starts when the bridge reads the event.
Needs: pip install boto3 apache-iggy==0.9.0, and AWS credentials that can read the queue.
"""
import argparse
import asyncio
import json
from collections import OrderedDict, defaultdict

import boto3
from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, STREAM, USAGE, cycle, ensure_topics, msg, part


def to_usage(body: str, a):
    d = json.loads(body)
    if "Message" in d and isinstance(d["Message"], str):
        d = json.loads(d["Message"])        # delivered through SNS
    if a.meter and d.get(a.meter_field) != a.meter:
        return None                         # another meter: this lane tracks one
    return (str(d[a.app_field]), str(d.get(a.source_field, "unknown")), str(d[a.id_field]),
            float(d[a.value_field]))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, USAGE)
    producer = await iggy.producer(STREAM, USAGE, topic_partitions_count=PARTITIONS[USAGE])
    sqs = boto3.client("sqs", region_name=a.region)
    seen = OrderedDict()                    # recent usageUniqueIds, so a redelivered event adds nothing
    seq, cum = defaultdict(int), defaultdict(float)
    async with producer:
        while True:
            r = await asyncio.to_thread(sqs.receive_message, QueueUrl=a.queue_url, MaxNumberOfMessages=10,
                                        WaitTimeSeconds=1)
            got = r.get("Messages", [])
            c = cycle(a.cycle_s)
            groups = defaultdict(list)
            for m in got:
                try:
                    u = to_usage(m["Body"], a)
                except (KeyError, ValueError):
                    continue                # not a usage event this mapping understands
                if not u or u[2] in seen:
                    continue
                name, src, uid, gbs = u
                seen[uid] = None
                if len(seen) > 2_000_000:
                    seen.popitem(last=False)
                seq[(src, name)] += 1
                cum[(c, src, name)] += gbs
                groups[part(name, PARTITIONS[USAGE])].append(SendMessage(msg(
                    h=src, a=name, c=c, seq=seq[(src, name)], id=uid, gbs=round(cum[(c, src, name)], 5), d=gbs)))
            # One batch per partition, all sent at once, so an app's usage stays in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))
            if got:                         # only after Iggy has the batch: our own queue, nobody else's
                await asyncio.to_thread(sqs.delete_message_batch, QueueUrl=a.queue_url, Entries=[
                    {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(got)])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue-url", required=True, help="the SQS queue your shadow StreamHub subscription delivers to")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--app-field", default="appId")
    ap.add_argument("--source-field", default="source")
    ap.add_argument("--id-field", default="usageUniqueId")
    ap.add_argument("--value-field", default="value", help="GB-seconds in this event")
    ap.add_argument("--meter-field", default="meter")
    ap.add_argument("--meter", default="", help="keep only this meter, e.g. your Forge Functions duration meter")
    ap.add_argument("--cycle-s", type=float, default=0, help="0 = the calendar month, as Forge bills")
    ap.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(ap.parse_args()))
