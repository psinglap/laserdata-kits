"""Shadow your FBF inventory table's CockroachDB changefeed from Kafka into the stock lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the changefeed, the services that write the table, and
every existing consumer (the Iceberg ingestion included) are untouched. It writes, keyed by brand:SKU, the resulting
levels of each SKU (on hand, reserved, available, case pack), so checkout.py can run on your real traffic.

Expects a changefeed in the default wrapped JSON envelope with the updated option, for example:
  CREATE CHANGEFEED FOR TABLE fbf_inventory INTO 'kafka://…' WITH updated, resolved = '10s';
Each record's "updated" timestamp (the row's commit time in CockroachDB) becomes the per-SKU sequence, so the
redelivered duplicates a changefeed can emit are dropped here and in checkout.py, and only the newest levels win.
Map your column names in to_change().

Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for SASL/SCRAM on MSK:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STOCK, STREAM, msg, part_of


def hlc(updated: str) -> tuple[int, int]:
    # "1729600000123456789.0000000002" -> (wall-clock ns, sequence): wall time first, then the logical counter.
    wall, _, logical = updated.partition(".")
    return int(wall), int(wall) * 10**10 + int(logical or 0)


def to_change(key: bytes, value: bytes):
    """Your table's shape goes here. Return (brand:SKU, levels, updated) or None to skip a record.
    This default expects columns brand_token, sku, on_hand, reserved, case_pack."""
    d = json.loads(value)
    if "updated" not in d or "resolved" in d:
        return None                          # resolved timestamps carry no row
    row = d.get("after")
    if row is None:                          # a deleted row: nothing left to sell
        brand, sku = json.loads(key)[:2]
        return f"{brand}:{sku}", {"on_hand": 0, "reserved": 0, "avail": 0, "case": 1}, d["updated"]
    levels = {"on_hand": row["on_hand"], "reserved": row["reserved"], "avail": row["on_hand"] - row["reserved"],
              "case": row.get("case_pack") or 1}
    return f"{row['brand_token']}:{row['sku']}", levels, d["updated"]


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, STOCK, topic_partitions_count=PARTITIONS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True, "auto.commit.interval.ms": 1000}   # offsets once a second
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    last = defaultdict(int)                  # newest commit timestamp forwarded per SKU
    forwarded, dropped, t0 = 0, 0, time.monotonic()
    async with producer:
        while True:
            batches = defaultdict(list)
            for rec in kafka.consume(num_messages=2000, timeout=0.005):
                if rec.error():
                    continue
                got = to_change(rec.key(), rec.value())
                if got is None:
                    continue
                k, levels, updated = got
                wall, seq = hlc(updated)
                if seq <= last[k]:
                    dropped += 1             # a redelivered or older version of this row
                    continue
                last[k] = seq
                ns = wall if a.ns_from_commit else time.time_ns()
                batches[part_of(k)].append(SendMessage(msg(k=k, seq=seq, ns=ns, kind="shadow", **levels)))
            if batches:
                # One batch per partition, all sent at once; a SKU's records keep their order.
                await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in batches.items()))
                forwarded += sum(len(ms) for ms in batches.values())
            if time.monotonic() - t0 >= 1:
                print(f"[bridge] {forwarded:,} stock changes/s forwarded from '{a.kafka_topic}'  "
                      f"duplicates dropped {dropped:,}", flush=True)
                forwarded, dropped, t0 = 0, 0, time.monotonic()
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", required=True, help="the changefeed topic of your FBF inventory table")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--ns-from-commit", action="store_true",
                   help="time latency from the row's commit in CockroachDB instead of from the bridge")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
