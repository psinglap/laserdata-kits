"""Shadow your ad segment or tracker records from Amazon Kinesis Data Streams into the tracker lane, read-only.

It reads one Kinesis stream shard by shard with GetRecords, from LATEST, and never writes to Kinesis: the
stitchers, the trackers you run today and every existing consumer are untouched. Each record that describes an
ad segment served to a session is published to ad-segments, keyed by session, with the same partitioning as the
kit, one batch per partition per read. Point tracker.py at Iggy and compare its beacon counts with yours.

Kinesis limits each shard to 5 GetRecords calls and 2 MB/s a second, shared by every polling consumer. This
bridge makes at most one call per --interval per shard (default 0.2 s, i.e. up to 5 a second), so on a stream
whose consumers already use that budget, run it on a copy of the stream or for a short window, or register it as
an enhanced fan-out consumer.

Map your records in to_segment(): the kit needs the session, the impression (or the ad break and slot), the
creative, the segment's index in the ad, and the ad's length and segment length. If your records are written in
periodic batches, a shadow run checks the beacon logic and the counts, not the latency: the clock starts when
the bridge reads the record.
Needs: pip install boto3 apache-iggy==0.9.0, and AWS credentials allowed kinesis:ListShards,
kinesis:GetShardIterator and kinesis:GetRecords on the stream.
"""
import argparse
import asyncio
import json
import math
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, SEGMENTS, STREAM, ensure_topics, msg, part


def to_segment(data: bytes):
    """Return a lane message's fields for one ad segment served, or None for any other record."""
    d = json.loads(data)
    # Rename your fields here, for example sessionId -> s and adId -> ad.
    s = d.get("session") or d.get("sessionId") or d.get("session_id")
    g = d.get("segment") if d.get("segment") is not None else d.get("segmentIndex")
    if s is None or g is None:
        return None
    ad_ms = int(float(d.get("adDuration") or d.get("ad_duration") or 30) * 1000)
    seg_ms = int(float(d.get("segmentDuration") or d.get("segment_duration") or 2) * 1000)
    imp = str(d.get("impression") or d.get("impressionId") or f"{s}:{d.get('breakId', '')}:{d.get('slot', '')}")
    now = time.time_ns()
    return {"s": str(s), "ch": str(d.get("channel") or d.get("channelId") or ""), "i": imp,
            "ad": str(d.get("ad") or d.get("adId") or d.get("creativeId") or ""), "g": int(g),
            "n": math.ceil(ad_ms / seg_ms), "d": ad_ms, "sm": seg_ms,
            "a0": now - int(g) * seg_ms * 1_000_000, "ns": now}


async def main(a):
    import boto3                                    # only the bridge needs it

    kin = boto3.client("kinesis", region_name=a.region)
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, SEGMENTS)
    shards, token = [], None
    while True:                                     # every open shard of the stream
        r = kin.list_shards(NextToken=token) if token else kin.list_shards(StreamName=a.stream)
        shards += [s["ShardId"] for s in r["Shards"] if "EndingSequenceNumber" not in s["SequenceNumberRange"]]
        token = r.get("NextToken")
        if not token:
            break
    print(f"[bridge] reading {len(shards)} shards of {a.stream} from LATEST, read-only", flush=True)
    lock = asyncio.Lock()
    count = [0]

    async def shard_loop(shard):
        it = kin.get_shard_iterator(StreamName=a.stream, ShardId=shard, ShardIteratorType="LATEST")["ShardIterator"]
        while it:
            r = await asyncio.to_thread(kin.get_records, ShardIterator=it, Limit=10_000)
            it = r.get("NextShardIterator")
            groups = defaultdict(list)
            for rec in r["Records"]:
                e = to_segment(rec["Data"])
                if e:
                    groups[part(e["s"], PARTITIONS[SEGMENTS])].append(SendMessage(msg(**e)))
            if groups:
                async with lock:
                    await asyncio.gather(*(iggy.send_messages(STREAM, SEGMENTS, Partitioning.partition_id(pid), ms)
                                           for pid, ms in groups.items()))
                    count[0] += sum(len(ms) for ms in groups.values())
            await asyncio.sleep(a.interval)         # stay inside the shard's 5 reads a second

    async def report():
        while True:
            await asyncio.sleep(10)
            print(f"[bridge] {count[0]:,} ad segments copied", flush=True)

    await asyncio.gather(report(), *(shard_loop(s) for s in shards))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--stream", required=True, help="your Kinesis stream with ad segment or tracker records")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--interval", type=float, default=0.2, help="seconds between GetRecords calls per shard")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
