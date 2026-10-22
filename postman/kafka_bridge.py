"""Shadow your witness batches from Kafka into the Apache Iggy witness lane, read-only.

async_witnesses publishes what the agents upload to Kafka, and witness_assembler consumes it (the stages
asw_kafka_published and asm_kafka_received in the agent's EventTimestamps). This bridge reads that topic as
its own consumer group ("iggy-shadow"), so the agents, async_witnesses, witness_assembler and ClickHouse are
untouched. It republishes each batch onto the lane keyed by service, the same shape async_witnesses.py
writes, so assembler.py can read it next to your assembler.

Map your record in to_batch(). The public agent code shows the upload body (UploadReportsRequest: client_id
and witnesses, each with an id, witness_proto and optional event_timestamps), but not what async_witnesses
puts on Kafka, so the defaults are guesses to adjust:
  * a record whose JSON has a "witnesses" list is one batch; any other record is one witness, and the
    witnesses of one client read in one poll are grouped into a batch;
  * the service comes from --service-field, else the record key;
  * the batch number comes from --batch-field when your records carry one; today's upload body has none, so
    by default the bridge numbers each client's batches in the order Kafka delivers them, and missing-batch
    naming then covers only what reached Kafka (the kit's case for an agent-side number is in the README);
  * endpoint, status and latency come from --endpoint/--status/--latency fields if your records carry them
    decoded; witness_proto itself is not decoded here, so without them only the counts are meaningful.
In a shadow run only the lane is timed; the clock starts when the bridge publishes the batch.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, PARTITIONS, STREAM, WITNESSES, ensure_topics, msg, part


def witness(w: dict, a) -> list:
    return [w.get(a.endpoint_field, 0), int(w.get(a.status_field, 0) or 0), float(w.get(a.latency_field, 0) or 0)]


def to_batch(rec, a):
    """Return (client_id, service, batch number or None, [witness, ...], is_whole_batch)."""
    d = json.loads(rec.value())
    key = rec.key().decode() if rec.key() else "unknown"
    svc = str(d.get(a.service_field) or key)
    client = str(d.get("client_id", "unknown"))
    b = d.get(a.batch_field)
    if isinstance(d.get("witnesses"), list):
        return client, svc, b, [witness(w, a) for w in d["witnesses"]], True
    return client, svc, b, [witness(d, a)], False


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, WITNESSES)
    producer = await iggy.producer(STREAM, WITNESSES, topic_partitions_count=PARTITIONS[WITNESSES])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.topic])
    bseq = defaultdict(int)                 # client -> last batch number the bridge assigned
    async with producer:
        while True:
            batches, loose = [], defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.005):
                if rec.error():
                    continue
                try:
                    client, svc, b, ws, whole = to_batch(rec, a)
                except (KeyError, ValueError, TypeError, AttributeError):
                    continue                # not a record this mapping understands
                if whole:
                    batches.append((client, svc, b, ws))
                else:
                    loose[(client, svc)].extend(ws)
            batches += [(c, s, None, ws) for (c, s), ws in loose.items()]
            groups = defaultdict(list)
            for client, svc, b, ws in batches:
                if b is None:
                    bseq[client] += 1
                    b = bseq[client]
                groups[part(svc, PARTITIONS[WITNESSES])].append(SendMessage(msg(
                    c=client, s=svc, b=int(b), up=time.time_ns(), w=ws)))
            # One send per partition, all at once, so a service's batches stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="your Kafka brokers, host:port")
    ap.add_argument("--topic", required=True, help="the topic async_witnesses publishes to")
    ap.add_argument("--service-field", default="service_id", help="falls back to the record key")
    ap.add_argument("--batch-field", default="batch", help="the agent's batch number, if your records carry one")
    ap.add_argument("--endpoint-field", default="endpoint")
    ap.add_argument("--status-field", default="status")
    ap.add_argument("--latency-field", default="processing_latency", help="milliseconds")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
