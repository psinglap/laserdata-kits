"""Shadow your DSP responses from Kafka into the Apache Iggy appetite lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so the exchange, every producer and every existing
consumer (Spark Structured Streaming jobs, reporting, billing) are untouched. For each response it keeps
a recent bid rate per (DSP, supply segment) pair, and writes a change to the lane when a DSP stops or
starts bidding on a segment, or when the rate moves by more than --min-move. Each change carries the
resulting rate, whether the DSP is bidding, and a per-pair sequence, the same shape exchange.py writes.
Point shaper.py at Iggy with --replay-s 0 and compare its table with what your shaping model sees today.

Map your record in to_response(): the kit needs the DSP, the supply segment your shaping model keys on
(or the fields to build it from) and whether the DSP bid. The defaults read JSON with fields dsp_id,
segment and bid (true/false); --segment-fields joins several fields into one segment key instead.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import SendMessage
from confluent_kafka import Consumer

from common import APPETITE, DEFAULT_URL, PARTITIONS, STREAM, connect, ensure_topic, msg, part


def to_response(value: bytes, a):
    d = json.loads(value)
    seg = "|".join(str(d[f]) for f in a.segment_fields.split(",")) if a.segment_fields else str(d[a.segment_field])
    return f"{d[a.dsp_field]}|{seg}", bool(d[a.bid_field])


async def main(a):
    iggy = await connect(a.url)
    await ensure_topic(iggy, APPETITE)
    writers = [await connect(a.url) for _ in range(PARTITIONS[APPETITE])]
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    print(f"[bridge] {a.kafka_topic} on {a.bootstrap} -> Iggy {STREAM}/{APPETITE}", flush=True)
    rate = {}                                   # pair -> [ewma bid rate, bidding?, seq, rate last written, last bid ns]
    pending = defaultdict(list)
    alpha = 1 / a.window

    def on_response(k, bid):
        now = time.time_ns()
        s = rate.get(k)
        if s is None:
            s = rate[k] = [1.0 if bid else 0.0, bid, 0, -1.0, now if bid else 0]
        s[0] += alpha * ((1.0 if bid else 0.0) - s[0])
        if bid:
            s[4] = now
        # A DSP counts as not bidding on a segment after --off-after-s without a single bid there.
        on = bid or (now - s[4]) < a.off_after_s * 1e9
        flip = on != s[1]
        if flip or abs(s[0] - s[3]) >= a.min_move:
            s[1], s[3] = on, s[0]
            s[2] += 1
            pending[part(k)].append(SendMessage(msg(k, s[2], s[0] if on else 0.0, on, flip and s[2] > 1)))

    async def write(p):
        while True:
            if pending[p]:
                batch, pending[p] = pending[p], []
                await writers[p].send_messages(STREAM, APPETITE, p, batch)
            await asyncio.sleep(a.tick_ms / 1000)

    async def consume():
        while True:
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    on_response(*to_response(rec.value(), a))
                except (KeyError, ValueError, TypeError):
                    continue                    # not a response record this mapping understands
            await asyncio.sleep(0)

    await asyncio.gather(consume(), *(write(p) for p in range(PARTITIONS[APPETITE])))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    p.add_argument("--kafka-topic", default="dsp-responses", help="your DSP response or auction-outcome topic")
    p.add_argument("--dsp-field", default="dsp_id")
    p.add_argument("--segment-field", default="segment")
    p.add_argument("--segment-fields", default="", help="or several fields joined into one key, e.g. publisher_id,format,geo")
    p.add_argument("--bid-field", default="bid")
    p.add_argument("--window", type=float, default=200, help="responses in the bid-rate moving average")
    p.add_argument("--min-move", type=float, default=0.02, help="write a change when the rate moves this much")
    p.add_argument("--off-after-s", type=float, default=5, help="no bid on a pair for this long counts as not bidding")
    p.add_argument("--tick-ms", type=float, default=2)
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
