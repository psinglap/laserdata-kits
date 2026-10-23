"""Shadow your login and profile-change events from Kafka into the Apache Iggy graph link lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow"), so every producer, the graph data process service
and every existing consumer are untouched. It turns each event into account-to-asset links (device ID,
IP, VID, address) and publishes them to the lane, keyed by asset. Point graph.py at Iggy and compare.

Map your records in to_links(): the kit needs the account and its assets. Records keep the same asset
partitioning as the kit, so each asset's links stay in order. In a shadow run only the lane is timed;
the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example for the SASL port:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, LINKS, PARTITIONS, STREAM, ensure_topics, msg, part

KINDS = {"dev": ("device_id", "deviceId"), "ip": ("ip", "ipAddress"), "vid": ("vid", "visitorId"),
         "addr": ("address_id", "addressId")}


def to_links(value: bytes):
    d = json.loads(value)
    # Rename your fields here, for example accountNumber -> account. Hash or tokenize assets as your
    # data rules require; the lane only needs a stable ID per asset.
    acct = str(d.get("account") or d.get("accountId") or d.get("account_id") or "")
    out = []
    for kind, names in KINDS.items():
        for n in names:
            if d.get(n):
                out.append((f"{kind}:{d[n]}", kind))
                break
    return acct, out


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, LINKS)
    producer = await iggy.producer(STREAM, LINKS, topic_partitions_count=PARTITIONS[LINKS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe(a.kafka_topic)
    seq = time.time_ns()                    # one rising counter for every edge, still higher after a restart
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                acct, links = to_links(rec.value())
                if not acct:
                    continue
                for asset, kind in links:
                    seq += 1
                    groups[part(asset, PARTITIONS[LINKS])].append(
                        SendMessage(msg(asset=asset, kind=kind, acct=acct, seq=seq, linked=1)))
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                   for pid, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="a broker of the Kafka cluster that carries the events")
    p.add_argument("--kafka-topic", required=True, action="append", help="a login or profile-change topic (repeatable)")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
