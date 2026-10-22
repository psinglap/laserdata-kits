"""Shadow your playback start and stop events from Amazon Kinesis Data Streams into the watch signals lane, read-only.

It reads one Kinesis stream shard by shard with GetRecords, from LATEST, and never writes to Kinesis:
the apps, the Event Platform, and every existing consumer and Flink application are untouched. For each
read batch it keeps each profile's latest state (resulting value, not a delta), adds a sequence (the read
time in ns, so it only grows, even across bridge restarts), and publishes to the lane keyed by profile,
with the same partitioning as the kit. Point features.py at Iggy and compare.

Kinesis limits each shard to 5 GetRecords calls and 2 MB/s a second, shared by every polling consumer.
This bridge makes at most --interval-spaced calls per shard (default 0.2 s, i.e. up to 5 a second), so on
a stream whose consumers already use that budget, run it on a copy of the stream or for a short window,
or register it as an enhanced fan-out consumer.

Map your records in to_signal(): the kit needs the profile ID, and the live event the profile is now
watching (empty when it stopped). In a shadow run only the lane is timed; the clock starts when the
bridge reads the record.
Needs: pip install boto3 apache-iggy==0.9.0, and AWS credentials allowed kinesis:ListShards,
kinesis:GetShardIterator and kinesis:GetRecords on the stream.
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, STREAM, WATCH, ensure_topics, msg, part


def to_signal(data: bytes):
    d = json.loads(data)
    # Rename your fields here, for example profileId -> profile, and the airing or content ID of a live event.
    profile = str(d.get("profile") or d.get("profileId") or d.get("profile_id"))
    kind = str(d.get("act") or d.get("type") or d.get("eventType") or "").lower()
    event = str(d.get("event") or d.get("airingId") or d.get("contentId") or "")
    now = "" if "stop" in kind or "end" in kind else event
    return profile, now


async def main(a):
    import boto3                                    # only the bridge needs it

    kin = boto3.client("kinesis", region_name=a.region)
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, WATCH)
    producer = await iggy.producer(STREAM, WATCH, topic_partitions_count=PARTITIONS[WATCH])
    shards, token = [], None
    while True:                                     # every open shard of the stream
        r = kin.list_shards(NextToken=token) if token else kin.list_shards(StreamName=a.stream)
        shards += [s["ShardId"] for s in r["Shards"] if "EndingSequenceNumber" not in s["SequenceNumberRange"]]
        token = r.get("NextToken")
        if not token:
            break
    print(f"[bridge] reading {len(shards)} shards of {a.stream} from LATEST, read-only", flush=True)
    watching = {}                                   # profile -> event it was watching, to fill in "left"
    lock = asyncio.Lock()

    async def shard_loop(shard):
        it = kin.get_shard_iterator(StreamName=a.stream, ShardId=shard, ShardIteratorType="LATEST")["ShardIterator"]
        while it:
            r = await asyncio.to_thread(kin.get_records, ShardIterator=it, Limit=10_000)
            it = r.get("NextShardIterator")
            latest = {}
            for rec in r["Records"]:
                profile, now = to_signal(rec["Data"])
                latest[profile] = now               # keep each profile's latest state in this read
            if latest:
                async with lock:
                    s = time.time_ns()              # one signal per profile per read, newer than any before
                    groups = defaultdict(list)
                    for profile, now in latest.items():
                        left = watching.get(profile, "")
                        if now:
                            watching[profile] = now
                        else:
                            watching.pop(profile, None)
                        act = "stop" if not now else "start" if not left else "switch"
                        groups[part(profile, PARTITIONS[WATCH])].append(
                            SendMessage(msg(profile=profile, seq=s, act=act, now=now, left=left)))
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                           for pid, ms in groups.items()))
            await asyncio.sleep(a.interval)         # stay inside the shard's 5 reads a second

    async with producer:
        await asyncio.gather(*(shard_loop(s) for s in shards))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--stream", required=True, help="your Kinesis stream with playback start and stop events")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--interval", type=float, default=0.2, help="seconds between GetRecords calls per shard")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
