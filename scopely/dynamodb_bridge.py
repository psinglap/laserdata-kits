"""Shadow your race progress from a DynamoDB table's stream into the race lane, read-only.

The game servers already write each player's race progress to DynamoDB. With DynamoDB Streams enabled on that
table (NEW_IMAGE; a table setting, no change to the game server), this bridge reads the stream shard by shard
from LATEST and publishes one roll per changed item to race-rolls, keyed by race, with the same partitioning as
the kit. It never writes to DynamoDB: no Lambda, no materialized view in a table, so the table's write units do
not grow. Point race_owner.py at Iggy and compare its laps, finishes and places with what the game shows.

DynamoDB Streams serves each shard to at most two readers at a time without throttling, so on a table whose
stream already has two consumers, run this on a copy or for a short window. Shards close and split over time;
the bridge looks for new shards every --rescan-s seconds and follows their children.

Map your item in to_roll(): the kit needs the race, the race number, the team, the player and the player's points
in this race (the resulting value). The stream's sequence number is the roll sequence: it only grows for an item,
so a record the stream delivers twice counts once. In a shadow run only the lane is timed; the clock starts when
the bridge reads the record.
Needs: pip install boto3 apache-iggy==0.9.0, and AWS credentials allowed dynamodb:DescribeStream,
dynamodb:GetShardIterator and dynamodb:GetRecords on the stream.
"""
import argparse
import asyncio
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, ROLLS, STREAM, ensure_topics, msg, part


def to_roll(item: dict, seq: int):
    # Rename your attributes here, for example raceId -> r, raceNumber -> rn, teamSlot -> tm, playerId -> p,
    # racePoints -> tot. Return None for items that are not race progress.
    if "raceId" not in item and "race" not in item:
        return None
    return {"r": str(item.get("raceId") or item.get("race")), "rn": int(item.get("raceNumber", item.get("rn", 1))),
            "tm": int(item.get("teamSlot", item.get("team", 0))), "p": str(item.get("playerId") or item.get("player")),
            "s": "bridge", "seq": seq, "tot": int(item.get("racePoints", item.get("points", 0)))}


async def main(a):
    import boto3                                    # only the bridge needs it
    from boto3.dynamodb.types import TypeDeserializer

    ds = boto3.client("dynamodbstreams", region_name=a.region)
    des = TypeDeserializer()
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, ROLLS)
    producer = await iggy.producer(STREAM, ROLLS, topic_partitions_count=PARTITIONS[ROLLS])
    started = set()

    def open_shards():
        out, start = [], None
        while True:
            kw = {"StreamArn": a.stream_arn, **({"ExclusiveStartShardId": start} if start else {})}
            d = ds.describe_stream(**kw)["StreamDescription"]
            out += [s["ShardId"] for s in d["Shards"] if "EndingSequenceNumber" not in s["SequenceNumberRange"]]
            start = d.get("LastEvaluatedShardId")
            if not start:
                return out

    async def shard_loop(shard):
        it = ds.get_shard_iterator(StreamArn=a.stream_arn, ShardId=shard,
                                   ShardIteratorType="LATEST")["ShardIterator"]
        while it:                                   # a closed shard ends with no next iterator
            r = await asyncio.to_thread(ds.get_records, ShardIterator=it, Limit=1000)
            it = r.get("NextShardIterator")
            groups = defaultdict(list)
            for rec in r["Records"]:
                img = rec["dynamodb"].get("NewImage")
                if not img:
                    continue                        # a delete: nothing to score
                d = to_roll({k: des.deserialize(v) for k, v in img.items()}, int(rec["dynamodb"]["SequenceNumber"]))
                if d:
                    groups[part(d["r"], PARTITIONS[ROLLS])].append(SendMessage(msg(**d)))
            if groups:
                await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in groups.items()))
            await asyncio.sleep(a.interval)

    async with producer:
        tasks = []
        while True:
            for s in open_shards():
                if s not in started:
                    started.add(s)
                    tasks.append(asyncio.create_task(shard_loop(s)))
            print(f"[bridge] reading {len(started)} shards of the race table's stream from LATEST, read-only",
                  flush=True)
            await asyncio.sleep(a.rescan_s)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--stream-arn", required=True, help="the DynamoDB stream ARN of your race progress table")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--interval", type=float, default=0.25, help="seconds between GetRecords calls per shard")
    p.add_argument("--rescan-s", type=float, default=60, help="seconds between looks for new shards")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
