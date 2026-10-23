"""Shadow your time waves into the time-wave lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the pipeline services, the alerting pipeline and every
other consumer are untouched. It reads two kinds of record from your Kafka topics:

  emitted   a time-wave generation sent into a cell's pipeline: cell, source pipeline, generation and how many
            synthetic points it carries (one per Kafka partition). If your generator does not write these to a
            topic, a few lines in the generator can write {k, g, x} to wave-lane directly.
  arrived   a synthetic point seen at the end of the pipeline: cell, source pipeline, generation and the Kafka
            partition it was sent through.

Every --flush-ms it writes one tally per cell/pipeline/generation naming the partitions whose points arrived,
keyed by cell/pipeline, and the emitted records as they come. watermark.py can then run on your real waves.
The bridge sits outside the cell it reads: run it next to the watermark service, not on the cell's brokers.

Map your records in to_wave(): return a dict for a time-wave record, or None to skip it.

Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SSL
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, ensure_topics, msg, part, wave_key


def to_wave(rec):
    """Your record shape goes here. This default expects JSON values such as
    {"cell": "cell-07", "pipeline": "logs", "generation": 1761200000, "points": 64}       (emitted) or
    {"cell": "cell-07", "pipeline": "logs", "generation": 1761200000, "partition": 17}    (arrived)."""
    try:
        v = json.loads(rec.value())
        k, g = wave_key(v["cell"], v["pipeline"]), int(v["generation"])
    except (ValueError, KeyError, TypeError):
        return None
    if "points" in v:
        return {"k": k, "g": g, "x": int(v["points"])}
    if "partition" in v:
        return {"k": k, "g": g, "p": int(v["partition"])}
    return None


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, LANE)
    lane = await iggy.producer(STREAM, LANE, topic_partitions_count=PARTITIONS[LANE])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True, "auto.commit.interval.ms": 1000}   # offsets once a second
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([t for t in (a.emit_topic, a.arrival_topic) if t])
    emitted, arrived = [], defaultdict(set)    # since the last flush
    forwarded, t0, next_flush = 0, time.monotonic(), time.monotonic()
    async with lane:
        while True:
            for rec in kafka.consume(num_messages=5000, timeout=0.005):
                if rec.error():
                    continue
                w = to_wave(rec)
                if w is None:
                    continue
                if "x" in w:
                    emitted.append(w)
                else:
                    arrived[(w["k"], w["g"])].add(w["p"])
            now = time.monotonic()
            if now >= next_flush and (emitted or arrived):
                next_flush = now + a.flush_ms / 1000
                batches = defaultdict(list)
                for w in emitted:          # emitted before tallies, so a generation is known before it fills
                    batches[part(w["k"], PARTITIONS[LANE])].append(SendMessage(msg(**w)))
                for (k, g), ps in arrived.items():
                    batches[part(k, PARTITIONS[LANE])].append(SendMessage(msg(k=k, g=g, p=sorted(ps))))
                emitted.clear()
                arrived.clear()
                # One batch per partition, all sent at once; a cell pipeline's records keep their order.
                await asyncio.gather(*(lane.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in batches.items()))
                forwarded += sum(len(ms) for ms in batches.values())
            if now - t0 >= 1:
                print(f"[bridge] {forwarded:,} wave records/s forwarded", flush=True)
                forwarded, t0 = 0, now
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--arrival-topic", required=True, help="where time-wave points are seen at the end of the pipeline")
    p.add_argument("--emit-topic", default="", help="where your generator records emitted generations, if anywhere")
    p.add_argument("--flush-ms", type=float, default=20, help="one tally per generation per this many ms")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
