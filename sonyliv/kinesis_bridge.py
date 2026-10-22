"""Shadow your Play Along locks from Amazon Kinesis Data Streams into the answer lane, read-only.

Sony LIV's real-time path sends priority events (playback failures, user interactions, live sports
engagement signals) through Kinesis Data Streams to ClickHouse (AWS Big Data Blog, Sep 2026). This bridge
reads one Kinesis stream shard by shard with GetRecords, from LATEST, and never writes to Kinesis: the
apps, the EKS ingest services, ClickPipes and every existing consumer are untouched. It forwards every
Play Along lock it finds (each one counts, so nothing is collapsed), keyed by player, with the same
partitioning as the kit, and the question timeline (open, close, reveal) if your stream carries it.
Point scorer.py at Iggy and compare its scores with your backend's.

Kinesis limits each shard to 5 GetRecords calls and 2 MB/s a second, shared by every polling consumer.
This bridge makes at most --interval-spaced calls per shard (default 0.2 s, i.e. up to 5 a second), so on
a stream whose consumers already use that budget, run it on a copy of the stream or for a short window,
or register it as an enhanced fan-out consumer.

Map your records in to_event(). A lock needs the player ID, the question ID, the option locked and the
time your answer API received it (the record's arrival time is used if the event has none). If your
question timeline is not on this stream, publish it to the show-desk topic from wherever it lives, in the
format of episode.py: {"kind": "open", "q", "level", "points", "timer_s", "answer"}, {"kind": "close", "q"},
{"kind": "reveal", "q", "answer"}, each with "ns". In a shadow run the clock starts when the bridge reads
the record.
Needs: pip install boto3 apache-iggy==0.9.0, and AWS credentials allowed kinesis:ListShards,
kinesis:GetShardIterator and kinesis:GetRecords on the stream.
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, LOCKS, PARTITIONS, SHOW, STREAM, ensure_topics, msg, part


def to_event(data: bytes, arrived_ns: int):
    """Return ("lock", fields), ("show", fields) or None for records the lane doesn't need."""
    d = json.loads(data)
    kind = str(d.get("event") or d.get("eventType") or d.get("type") or "").lower()
    # Rename your fields here, for example userId -> player, questionId -> q, selectedOption -> opt.
    if "lock" in kind or ("answer" in kind and "reveal" not in kind):
        return "lock", {"player": str(d.get("player") or d.get("userId") or d.get("user_id")),
                        "q": str(d.get("q") or d.get("questionId") or d.get("question_id")),
                        "opt": str(d.get("opt") or d.get("option") or d.get("selectedOption")),
                        "lock_ns": int(d.get("lock_ns") or d.get("receivedAtNs") or arrived_ns)}
    for k in ("open", "close", "reveal"):
        if f"question_{k}" in kind or f"question{k}" in kind or (k == "reveal" and "reveal" in kind):
            return "show", {"kind": k, **{f: d[f] for f in ("q", "level", "points", "timer_s", "answer") if f in d}}
    return None


async def main(a):
    import boto3                                    # only the bridge needs it

    kin = boto3.client("kinesis", region_name=a.region)
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, LOCKS, SHOW)
    lock_p = await iggy.producer(STREAM, LOCKS, topic_partitions_count=PARTITIONS[LOCKS])
    show_p = await iggy.producer(STREAM, SHOW, topic_partitions_count=PARTITIONS[SHOW])
    shards, token = [], None
    while True:                                     # every open shard of the stream
        r = kin.list_shards(NextToken=token) if token else kin.list_shards(StreamName=a.stream)
        shards += [s["ShardId"] for s in r["Shards"] if "EndingSequenceNumber" not in s["SequenceNumberRange"]]
        token = r.get("NextToken")
        if not token:
            break
    print(f"[bridge] reading {len(shards)} shards of {a.stream} from LATEST, read-only", flush=True)
    next_seq = iter(range(time.time_ns(), 1 << 63))   # only grows, even across bridge restarts
    lock = asyncio.Lock()

    async def shard_loop(shard):
        it = kin.get_shard_iterator(StreamName=a.stream, ShardId=shard, ShardIteratorType="LATEST")["ShardIterator"]
        while it:
            r = await asyncio.to_thread(kin.get_records, ShardIterator=it, Limit=10_000)
            it = r.get("NextShardIterator")
            locks, show = defaultdict(list), []
            for rec in r["Records"]:
                ev = to_event(rec["Data"], int(rec["ApproximateArrivalTimestamp"].timestamp() * 1e9))
                if ev is None:
                    continue
                if ev[0] == "lock":
                    f = ev[1]
                    locks[part(f["player"], PARTITIONS[LOCKS])].append((f, None))
                else:
                    show.append(SendMessage(msg(**ev[1])))
            if locks or show:
                async with lock:                    # one sequence order across shards
                    if show:
                        await show_p.send(show)
                    batches = {pid: [SendMessage(msg(seq=next(next_seq), **f)) for f, _ in fs]
                               for pid, fs in locks.items()}
                    await asyncio.gather(*(lock_p.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                           for pid, ms in batches.items()))
            await asyncio.sleep(a.interval)         # stay inside the shard's 5 reads a second

    async with lock_p, show_p:
        await asyncio.gather(*(shard_loop(s) for s in shards))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--stream", required=True, help="your Kinesis stream with Play Along answer locks")
    p.add_argument("--region", default="ap-south-1")
    p.add_argument("--interval", type=float, default=0.2, help="seconds between GetRecords calls per shard")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
