"""Shadow an existing forecast topic on MSK into Apache Iggy, read-only.

Reads as its own consumer group ("iggy-shadow") and never commits offsets, so the forecasting models that produce
the topic, Rider Pricing, Driver Earnings and every other consumer are untouched. Works with any endpoint that
speaks the Kafka protocol (pass SASL, IAM or TLS settings with -X).

Point marketplace.py at Iggy and compare. Map your records in to_messages(): the kit needs, per region and
forecast run, chunks {"t":"c","r":region,"f":frame,"i":chunk index,"cells":[[geohash,[demand x12],[supply x12]],...]}
and then a seal {"t":"s","r":region,"f":frame,"of":chunk count,"ev":0}. If your topic carries one record per
geohash, set --cells-per-run to the number of cells a region's run has; the bridge groups them into chunks and
writes the seal when the run is complete. The record format of Lyft's forecast topic is not public; the field
names below are placeholders.

Needs: pip install apache-iggy==0.9.0 confluent-kafka
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, FORECASTS, STREAM, connect, ensure_topics, part_of


class Runs:
    """Groups per-geohash records into chunks and seals, per region and forecast run."""

    def __init__(self, cells_per_run: int, chunk: int):
        self.cells_per_run, self.chunk = cells_per_run, chunk
        self.pending = defaultdict(list)    # (region, frame) -> cells not yet sent
        self.sent = defaultdict(int)        # (region, frame) -> chunks sent
        self.seen = defaultdict(int)        # (region, frame) -> cells seen

    def to_messages(self, body: bytes) -> list[tuple[str, bytes]]:
        d = json.loads(body)                # map your field names here
        r, f = d["region"], int(d["run"])
        k = (r, f)
        self.pending[k].append([d["geohash"], d["demand"], d["supply"]])
        self.seen[k] += 1
        out = []
        done = self.seen[k] >= self.cells_per_run
        if len(self.pending[k]) >= self.chunk or done:
            out.append((r, json.dumps({"t": "c", "r": r, "f": f, "i": self.sent[k], "ns": time.time_ns(),
                                       "cells": self.pending.pop(k)}, separators=(",", ":")).encode()))
            self.sent[k] += 1
        if done:
            out.append((r, json.dumps({"t": "s", "r": r, "f": f, "of": self.sent.pop(k), "ev": 0,
                                       "ns": time.time_ns()}, separators=(",", ":")).encode()))
            del self.seen[k]
        return out


async def main(a):
    from confluent_kafka import Consumer

    iggy = await connect(a.url)
    await ensure_topics(iggy, FORECASTS)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest",
            "enable.auto.commit": False}    # leave no committed offsets behind
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    runs = Runs(a.cells_per_run, a.chunk)
    while True:
        recs = [r for r in kafka.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            batches = defaultdict(list)     # same partition function as the kit: one region, one partition, in order
            for rec in recs:
                for region, payload in runs.to_messages(rec.value()):
                    batches[part_of(region)].append(SendMessage(payload))
            await asyncio.gather(*(iggy.send_messages(STREAM, FORECASTS, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="MSK bootstrap brokers")
    p.add_argument("--topic", default="geohash-forecasts", help="the topic the forecasting models sync results to")
    p.add_argument("--cells-per-run", type=int, required=True, help="geohash records in one region's forecast run")
    p.add_argument("--chunk", type=int, default=200, help="cells per Iggy message")
    p.add_argument("-X", action="append", default=[], metavar="key=value",
                   help="extra client setting, e.g. -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
