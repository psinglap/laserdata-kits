"""Shadow one Ditto CDC Data Bridge from its Kafka topic into Apache Iggy, read-only.

Ditto CDC publishes each Data Bridge to a Kafka topic you read over mTLS on port 443, with a consumer group whose
ID starts with the bridge's consumer group prefix. This bridge reads as its own group, <prefix>-iggy-shadow, and
never commits offsets, so the Data Bridge and every consumer you run today are untouched. Each event is written to
the lane unchanged (plus ns, when the bridge saw it), keyed by document _id with the kit's partition function, so
per-document order holds; a requeryRequired event goes to every partition, so every mirror sees it.

Connection details come from the Ditto portal ("Kafka consumer info") or, on Ditto Server, from the
BigPeerDataBridge resource's status.kafka: endpoint, topic, consumerGroupPrefix and the two certificate secrets.
librdkafka reads the user certificate (user.p12) directly; give it the cluster CA as PEM:
    openssl pkcs12 -in ca.p12 -nokeys -out ca.pem -passin pass:<cluster certificate password>

Point mirror.py at Iggy and compare it with the system you feed from the bridge today.

Needs: pip install apache-iggy==0.9.0 confluent-kafka
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import CHANGES, DEFAULT_URL, PARTITIONS, STREAM, connect, dump, ensure_topics, event_key, part_of


async def main(a):
    from confluent_kafka import Consumer

    iggy = await connect(a.url)
    await ensure_topics(iggy, CHANGES)
    conf = {"bootstrap.servers": a.endpoint, "group.id": f"{a.group_prefix}-iggy-shadow",
            "auto.offset.reset": "latest", "enable.auto.commit": False,     # leave no committed offsets behind
            "security.protocol": "SSL", "ssl.ca.location": a.ca_pem,
            "ssl.keystore.location": a.user_p12, "ssl.keystore.password": a.user_password}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    while True:
        recs = [r for r in kafka.consume(num_messages=1000, timeout=0.001) if not r.error()]
        if recs:
            batches = defaultdict(list)     # same partition function as the kit: one document, one partition
            for r in recs:
                e = json.loads(r.value())
                e.setdefault("ns", time.time_ns())
                k = event_key(e)
                body = SendMessage(dump(e))
                for p in ([part_of(k)] if k is not None else range(PARTITIONS[CHANGES])):
                    batches[p].append(body)
            await asyncio.gather(*(iggy.send_messages(STREAM, CHANGES, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", required=True, help="the Data Bridge's Kafka endpoint, host:443")
    p.add_argument("--topic", required=True, help="the Data Bridge's topic")
    p.add_argument("--group-prefix", required=True, help="the Data Bridge's consumer group prefix")
    p.add_argument("--ca-pem", required=True, help="cluster CA certificate as PEM (converted from ca.p12)")
    p.add_argument("--user-p12", required=True, help="user certificate, PKCS12")
    p.add_argument("--user-password", required=True, help="user certificate password")
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
