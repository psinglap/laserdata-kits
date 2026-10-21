"""Shadow your events topic into the ingest freshness lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the ingest service, Retriever, SLO processing,
Service Maps, Anomaly Detection and every other consumer are untouched. For each record it reads, it notes
the dataset, the Kafka partition and the offset, and every --flush-ms it writes one receipt per dataset that
had records, keyed by dataset, with the newest partition and offset seen. freshness.py can then run on your
real traffic.

Progress: Retriever keeps its own offsets, so its progress has to come from Retriever (a few lines that
send {kp, node, off} to query-progress every half second). Consumers that use Kafka consumer groups, such
as SLO processing, can be shadowed from here: pass --progress-group <group> and the bridge reads that
group's committed offsets every --progress-ms and writes them as progress. It only reads them; it never
joins the group.

At full volume, run several bridges, each on a slice of partitions (--partitions 0-9, 10-19, ...) chosen so
that every team's partitions fall in one slice; receipt sequences are numbered per bridge.

Map your records in to_dataset(): return the dataset (team/environment/dataset) a record belongs to, or
None to skip it.

Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SSL
"""
import argparse
import asyncio
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer, TopicPartition

from common import DEFAULT_URL, PARTITIONS, PROGRESS, RECEIPTS, STREAM, ensure_topics, msg, part


def to_dataset(rec):
    """Your record shape goes here. This default uses the record key, or a 'dataset' header if there is one."""
    for k, v in rec.headers() or []:
        if k == "dataset":
            return v.decode()
    return rec.key().decode() if rec.key() else None


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, RECEIPTS, PROGRESS)
    rx = await iggy.producer(STREAM, RECEIPTS, topic_partitions_count=PARTITIONS[RECEIPTS])
    pg_client = IggyClient.from_connection_string(a.url)      # one connection per producer
    await pg_client.connect()
    pg = await pg_client.producer(STREAM, PROGRESS, topic_partitions_count=PARTITIONS[PROGRESS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True, "auto.commit.interval.ms": 1000}   # offsets once a second
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    if a.partitions:
        lo, hi = (int(x) for x in a.partitions.split("-"))
        kafka.assign([TopicPartition(a.kafka_topic, p) for p in range(lo, hi + 1)])
    else:
        kafka.subscribe([a.kafka_topic])
    watchers = []                          # read-only views of other groups' committed offsets
    for g in a.progress_group:
        wc = dict(conf, **{"group.id": g, "enable.auto.commit": False})
        watchers.append((g, Consumer(wc)))
    meta = kafka.list_topics(a.kafka_topic, timeout=10).topics[a.kafka_topic]
    tps = [TopicPartition(a.kafka_topic, p) for p in meta.partitions]
    seq = defaultdict(int)
    newest = {}                            # dataset -> (kafka partition, offset, record ns) since the last flush
    forwarded, t0 = 0, time.monotonic()
    next_flush = next_progress = time.monotonic()
    async with rx, pg:
        while True:
            for rec in kafka.consume(num_messages=5000, timeout=0.005):
                if rec.error():
                    continue
                ds = to_dataset(rec)
                if ds is None:
                    continue
                ns = rec.timestamp()[1] * 1_000_000 if a.ns_from_kafka else time.time_ns()
                newest[ds] = (rec.partition(), rec.offset(), ns)
            now = time.monotonic()
            if now >= next_flush and newest:
                next_flush = now + a.flush_ms / 1000
                batches = defaultdict(list)
                for ds, (kp, off, ns) in newest.items():
                    seq[ds] += 1
                    batches[part(ds, PARTITIONS[RECEIPTS])].append(
                        SendMessage(msg(ds=ds, seq=seq[ds], kp=kp, off=off, ns=ns)))
                newest.clear()
                # One batch per partition, all sent at once; a dataset's receipts keep their order.
                await asyncio.gather(*(rx.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in batches.items()))
                forwarded += sum(len(ms) for ms in batches.values())
            if watchers and now >= next_progress:
                next_progress = now + a.progress_ms / 1000
                batches = defaultdict(list)
                for node, (g, wc) in enumerate(watchers):
                    for tp in wc.committed(tps, timeout=5):
                        if tp.offset >= 0:   # committed offset = next to read, so offset - 1 is applied
                            batches[part(str(tp.partition), PARTITIONS[PROGRESS])].append(
                                SendMessage(msg(kp=tp.partition, node=node, group=g, off=tp.offset - 1)))
                await asyncio.gather(*(pg.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in batches.items()))
            if now - t0 >= 1:
                print(f"[bridge] {forwarded:,} receipts/s forwarded from '{a.kafka_topic}'", flush=True)
                forwarded, t0 = 0, now
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", required=True, help="your events topic")
    p.add_argument("--partitions", default="", help="a slice of Kafka partitions to read, e.g. 0-9 (default: all)")
    p.add_argument("--flush-ms", type=float, default=100, help="one receipt per dataset per this many ms")
    p.add_argument("--progress-group", action="append", default=[], help="a consumer group to report progress for")
    p.add_argument("--progress-ms", type=float, default=500, help="how often to read committed offsets")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--ns-from-kafka", action="store_true",
                   help="time latency from the Kafka record timestamp instead of from the bridge")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
