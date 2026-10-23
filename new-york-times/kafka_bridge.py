"""Shadow the denormalized log from Kafka into Apache Iggy, read-only.

Reads the Publishing Pipeline's denormalized log (the topic partitioned by top-level asset URI that the
Elasticsearch ingestion and other consumers already read) as one more consumer group, `iggy-shadow`, so the
Gateway, the Monolog, the denormalizer and every existing consumer are untouched. Writes each asset's versions
to its partition once per --tick-ms, batched per partition. Point one serving_node.py at Iggy and compare the
version it serves for an asset with what nytimes.com serves.

Map your records in to_version(): the kit expects, per record, the top-level asset URI, the bundled asset and,
if your records carry it, a version and whether it was written because a referenced asset changed. Records on
the denormalized log are protobuf; decode them with your generated classes where marked. A per-asset version is
counted here when the record has none. The created time is when the bridge received the record, so the latency
serving_node.py prints starts at the bridge, not at the editor's click.
Needs: pip install confluent-kafka apache-iggy==0.9.0
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, REPUBLISH, STREAM, connect, ensure_topic, pack, partition_of

version = defaultdict(int)


def to_version(key: bytes | None, value: bytes) -> tuple[str, bytes] | None:
    # Replace with your record layout. The record key is the top-level asset URI when the topic is partitioned by it.
    # Example (JSON for the demo; decode your protobuf here instead):
    # {"uri": "nyt://article/5d1e…", "headline": "…", "cause": "dependency", "asset": {…}}
    try:
        r = json.loads(value)
        uri = (key.decode() if key else None) or r["uri"]
        v = int(r["version"]) if "version" in r else version[uri] + 1
    except (ValueError, KeyError, TypeError, UnicodeDecodeError):
        return None
    version[uri] = v
    why = REPUBLISH if r.get("cause") == "dependency" else "update"
    return uri, pack(uri, v, why, str(r.get("headline", "")), 0)     # add the bundled asset if your nodes serve from it


async def main(a):
    from confluent_kafka import Consumer as KafkaConsumer

    iggy = await connect(a.url)
    await ensure_topic(iggy, LANE, PARTITIONS)
    writers = [await connect(a.url) for _ in range(PARTITIONS)]
    pending = defaultdict(list)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow",
            "auto.offset.reset": "latest", "enable.auto.commit": True}
    for kv in a.X:                                       # e.g. -X security.protocol=SSL -X ssl.ca.location=...
        k_, v_ = kv.split("=", 1)
        conf[k_] = v_
    k = KafkaConsumer(conf)
    k.subscribe([a.topic])
    print(f"[bridge] {a.topic} on {a.bootstrap} (group iggy-shadow) -> Iggy {STREAM}/{LANE}", flush=True)

    async def pump():
        loop = asyncio.get_running_loop()
        while True:
            recs = await loop.run_in_executor(None, lambda: k.consume(500, 0.05))
            for r in recs:
                if r.error() is None:
                    u = to_version(r.key(), r.value())
                    if u:
                        pending[partition_of(u[0])].append(SendMessage(u[1]))

    async def write(pid):
        while True:
            if pending[pid]:
                batch, pending[pid] = pending[pid], []
                await writers[pid].send_messages(STREAM, LANE, pid, batch)   # one batch per partition per tick
            await asyncio.sleep(a.tick_ms / 1000)

    await asyncio.gather(pump(), *(write(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="127.0.0.1:9092")
    p.add_argument("--topic", required=True, help="the denormalized log topic")
    p.add_argument("-X", action="append", default=[], help="extra librdkafka setting, key=value (mutual TLS etc.)")
    p.add_argument("--tick-ms", type=float, default=2)
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
