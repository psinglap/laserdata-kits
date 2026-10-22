"""Shadow your existing price-tick topic from Kafka into Apache Iggy, read-only.

Runs as its own Kafka consumer group ("iggy-shadow") on the tick topic, either on the on-premise
cluster the price capture writes to or on the AWS cluster MirrorMaker replicates into. The
capture engine, MirrorMaker, the candle consumer and every other consumer are untouched. Point
candles.py at Iggy and compare its tick-to-candle delay with the delay you meter today.

Map your records in to_tick(): the kit needs a scrip code, a price and the total volume for the day
(the fields of a tick in "Charting at Angel One"). A per-scrip sequence is added here. With
--time-from bridge (default) the timestamp is when the bridge read the tick, so the delay
candles.py prints starts at the bridge; --time-from tick keeps your capture timestamp instead.
Ticks keep the kit's scrip partitioning, so each scrip's ticks stay in order.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example: -X security.protocol=SSL
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STREAM, TICKS, connect, ensure_topic, pack, part

seq = defaultdict(int)


def to_tick(value: bytes, keep_ts: bool):
    d = json.loads(value)
    # Rename your fields here, for example token -> scrip_code, ltp -> price, vol_traded_today -> total_volume.
    scrip = int(d.get("scrip_code") or d.get("token"))
    price = int(round(float(d.get("price") or d.get("ltp")) * 100))       # paise
    volume = int(d.get("total_volume") or d.get("volume") or 0)
    ts = int(d["timestamp"]) * 1_000_000 if keep_ts and "timestamp" in d else time.time_ns()   # ms -> ns
    seq[scrip] += 1
    return scrip, pack(scrip, ts, price, volume, seq[scrip])


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, TICKS, PARTITIONS)
    writers = [await connect(a.url) for _ in range(PARTITIONS)]
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": True}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    print(f"[bridge] {a.kafka_topic} on {a.bootstrap} -> Iggy {STREAM}/{TICKS}", flush=True)
    while True:
        groups = defaultdict(list)
        for rec in kafka.consume(num_messages=5000, timeout=0.001):
            if not rec.error():
                scrip, b = to_tick(rec.value(), a.time_from == "tick")
                groups[part(scrip)].append(SendMessage(b))
        # One batch per partition, sent together on each partition's own connection.
        await asyncio.gather(*(writers[p].send_messages(STREAM, TICKS, p, ms) for p, ms in groups.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers for the tick topic")
    p.add_argument("--kafka-topic", required=True, help="your price-tick topic")
    p.add_argument("--time-from", choices=("bridge", "tick"), default="bridge")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
