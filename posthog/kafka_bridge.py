"""Shadow your events topic into the live events lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow") on the topic your livestream service reads today
(LIVESTREAM_CONSUMERS_EVENT_TOPIC), so capture, ingestion, livestream and every other consumer are untouched.
For each record it takes the `token` and `distinct_id` headers capture already writes, and from the body
the event name, `$pathname` and the capture time, and writes one compact record to the lane, keyed by
token, with a per-token sequence number. live.py can then run on your real traffic.

Set --client-rack to the pod's availability zone so fetches come from the closest replica, as livestream does.
At full volume, run several bridges on slices of Kafka partitions (--partitions 0-31, 32-63, ...), each with
its own --bridge-id: sequence numbers are per token per bridge, and live.py checks gaps per token and bridge.

Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SSL
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer, TopicPartition

from common import DEFAULT_URL, LANE, STREAM, ensure_topics, msg, part


def to_live(rec):
    """Your record shape goes here. Returns (token, distinct_id, event, pathname) or None to skip.
    Headers first (no parse needed), then the capture wrapper: {"token", "distinct_id", "data": "<event json>"}."""
    h = {k: v.decode() for k, v in (rec.headers() or []) if v is not None}
    tok, did = h.get("token"), h.get("distinct_id")
    event, path = h.get("event", ""), ""
    try:
        w = json.loads(rec.value())
        data = json.loads(w.get("data") or "{}")
        tok = tok or w.get("token") or data.get("api_key")
        did = did or str(w.get("distinct_id", ""))
        event = data.get("event", event)
        path = (data.get("properties") or {}).get("$pathname", "")
    except (ValueError, TypeError, AttributeError):
        pass
    return (tok, did, event, path) if tok else None


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    parts = await ensure_topics(iggy, LANE, partitions={LANE: a.lane_partitions})
    lane = await iggy.producer(STREAM, LANE, topic_partitions_count=parts)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True, "auto.commit.interval.ms": 1000}   # offsets once a second
    if a.client_rack:
        conf["client.rack"] = a.client_rack
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    if a.partitions:
        lo, hi = (int(x) for x in a.partitions.split("-"))
        kafka.assign([TopicPartition(a.kafka_topic, p) for p in range(lo, hi + 1)])
    else:
        kafka.subscribe([a.kafka_topic])
    seq = defaultdict(int)
    forwarded, skipped, t0 = 0, 0, time.monotonic()
    async with lane:
        while True:
            batches = defaultdict(list)
            for rec in kafka.consume(num_messages=5000, timeout=0.005):
                if rec.error():
                    continue
                live = to_live(rec)
                if live is None:
                    skipped += 1
                    continue
                tok, did, event, path = live
                seq[tok] += 1
                ns = rec.timestamp()[1] * 1_000_000 if a.ns_from_kafka else time.time_ns()
                batches[part(tok, parts)].append(SendMessage(msg(t=tok, s=seq[tok], d=did, e=event, p=path,
                                                                 b=a.bridge_id, ns=ns)))
            if batches:
                # One batch per partition, all sent at once; a project's events keep their order.
                await asyncio.gather(*(lane.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in batches.items()))
                forwarded += sum(len(ms) for ms in batches.values())
            if time.monotonic() - t0 >= 1:
                print(f"[bridge] {forwarded:,} events/s forwarded from '{a.kafka_topic}' (skipped {skipped:,})",
                      flush=True)
                forwarded, skipped, t0 = 0, 0, time.monotonic()
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", required=True, help="the events topic livestream reads")
    p.add_argument("--partitions", default="", help="a slice of Kafka partitions to read, e.g. 0-31 (default: all)")
    p.add_argument("--client-rack", default="", help="this pod's availability zone, for closest-replica fetches")
    p.add_argument("--bridge-id", type=int, default=0)
    p.add_argument("--lane-partitions", type=int, default=256, help="lane partitions, used if this creates the topic")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--ns-from-kafka", action="store_true",
                   help="time latency from the Kafka record timestamp instead of from the bridge")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
