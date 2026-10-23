"""Shadow updated datapoints from an Amazon Kinesis data stream into per-subscription Apache Iggy topics, read-only.

Samsara's engineering culture post says the ingestion team builds on AWS Kinesis; which stream the
Data Connectors read today is not public, so point this at any Kinesis stream that carries updated
GPS, diagnostics or event records with an org and a vehicle on each. It reads the stream shard by
shard with GetRecords, from LATEST, and never writes to Kinesis: the devices, the ingestion
services, the Data Connectors and every existing consumer are untouched. For each record it looks up
whether that org streams that entity (subscriptions.json, the same shape as the stream settings in
the dashboard) and writes it to that subscription's topic, keyed by vehicle ID, with the record's
id and a per-vehicle sequence. Then run: python connector.py --orgs 0   (reads every subscription
topic in the stream) and compare its lines with what your connectors deliver.

Kinesis allows each shard 5 GetRecords calls and 2 MB/s a second, shared by every polling consumer.
This bridge makes one call per --interval per shard (default 1 s), so it uses at most one of those
five; on a stream whose consumers already use the budget, register it as an enhanced fan-out
consumer or run it for a short window.

Needs: pip install boto3 apache-iggy==0.9.0, and AWS credentials allowed kinesis:ListShards,
kinesis:GetShardIterator and kinesis:GetRecords on the stream.
Map your records in to_datapoint(): the kit needs the org, the entity, the vehicle ID and the record id.

subscriptions.json:
  {"subscriptions": [{"org_id": "<org id>", "entities": ["gps", "events"]},
                     {"org_id": "<org id>", "entities": ["*"]}]}
"""
import argparse
import asyncio
import json
import re
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, STREAM, connect, ensure_topic, msg, part


def to_datapoint(data: bytes):
    """Return (org id, entity, vehicle id, record time ns, record id) from one of your records, or None to skip it."""
    d = json.loads(data)
    org = d.get("orgId") or d.get("org_id")
    obj = d.get("vehicle") or d.get("asset")         # Kafka Connector payloads carry {"vehicle": {"id": ...}}
    v = obj.get("id") if isinstance(obj, dict) else d.get("vehicleId") or d.get("vehicle_id")
    entity = d.get("entity") or ("events" if d.get("eventType") else None)
    if not (org and v and entity):
        return None
    return str(org), str(entity), str(v), int(d.get("ts_ns") or time.time_ns()), str(d.get("eventId") or d.get("id") or "")


async def main(a):
    import boto3                                    # only the bridge needs it

    kin = boto3.client("kinesis", region_name=a.region)
    iggy = await connect(a.url)
    subs = {str(s["org_id"]): set(s["entities"]) for s in json.load(open(a.config))["subscriptions"]}
    topics = {}                                      # (org, entity) -> partitions of its subscription topic
    seq, count = defaultdict(int), [0]
    shards, token = [], None
    while True:                                     # every open shard of the stream
        r = kin.list_shards(NextToken=token) if token else kin.list_shards(StreamName=a.stream)
        shards += [s["ShardId"] for s in r["Shards"] if "EndingSequenceNumber" not in s["SequenceNumberRange"]]
        token = r.get("NextToken")
        if not token:
            break
    print(f"[bridge] reading {len(shards)} shards of {a.stream} from LATEST, read-only", flush=True)
    lock = asyncio.Lock()

    async def topic_for(org, entity):
        if (org, entity) not in topics:
            t = re.sub(r"[^A-Za-z0-9_.-]", "_", f"org-{org}-{entity}")[:200]
            await ensure_topic(iggy, STREAM, t, a.partitions)
            topics[org, entity] = (t, (await iggy.get_topic(STREAM, t)).partitions_count)
        return topics[org, entity]

    async def shard_loop(shard):
        it = kin.get_shard_iterator(StreamName=a.stream, ShardId=shard, ShardIteratorType="LATEST")["ShardIterator"]
        while it:
            r = await asyncio.to_thread(kin.get_records, ShardIterator=it, Limit=10_000)
            it = r.get("NextShardIterator")
            async with lock:
                groups = defaultdict(list)
                for rec in r["Records"]:
                    dp = to_datapoint(rec["Data"])
                    if dp is None:
                        continue
                    org, entity, v, ns, rid = dp
                    wanted = subs.get(org, ())
                    if entity not in wanted and "*" not in wanted:
                        continue                     # this org does not stream this entity
                    t, parts = await topic_for(org, entity)
                    seq[t, v] += 1                   # per-vehicle order as read from Kinesis; restart = new run
                    groups[t, part(v, parts)].append(SendMessage(msg(ns=ns, id=rid, v=v, seq=seq[t, v], e=entity)))
                if groups:
                    await asyncio.gather(*(iggy.send_messages(STREAM, t, Partitioning.partition_id(pid), ms)
                                           for (t, pid), ms in groups.items()))
                    count[0] += sum(len(ms) for ms in groups.values())
            await asyncio.sleep(a.interval)         # stay well inside the shard's 5 reads a second

    async def report():
        while True:
            await asyncio.sleep(10)
            print(f"[bridge] {count[0]:,} datapoints copied into {len(topics)} subscription topics", flush=True)

    await asyncio.gather(report(), *(shard_loop(s) for s in shards))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--stream", required=True, help="your Kinesis stream with updated datapoints")
    p.add_argument("--config", required=True, help="subscriptions.json: which entities each org streams")
    p.add_argument("--region", default="us-west-2")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between GetRecords calls per shard")
    p.add_argument("--partitions", type=int, default=1, help="partitions for a new subscription topic")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
