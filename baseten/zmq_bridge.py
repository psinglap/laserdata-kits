"""Shadow your replicas' KV cache events into the Apache Iggy lane, read-only.

Engines that publish KV cache events over ZMQ PUB/SUB (vLLM and SGLang with KV events enabled, the transport
Dynamo's router reads with --event-plane zmq) accept any number of subscribers. This bridge is one more SUB
socket per replica endpoint: the engines, their current subscribers and the router are untouched. For each
event batch it publishes one lane message per event, keyed by deployment, with its own per-replica sequence,
the same shape serving.py writes. Point a router built on serving.py's Index at Iggy and compare its view with
your router's.

It also counts gaps in each engine's own sequence numbers: events this SUB socket never received. PUB/SUB
drops messages under load; on the lane, once an event is in the log, every router reads it in order.

Decoding follows vLLM's KV event format (multipart: topic, 8-byte big-endian sequence, msgpack EventBatch of
tagged arrays: BlockStored, BlockRemoved, AllBlocksCleared). Check it against your engine version and adapt
to_events(). For TensorRT-LLM, whose executor returns KV cache events through its API, send the same lane message
(the body of publish() in main) from the loop that already reads them.
In a shadow run only the lane is timed; the clock starts when the bridge receives the batch.
Needs: pip install pyzmq msgpack apache-iggy==0.9.0
"""
import argparse
import asyncio
from collections import defaultdict

import msgpack
import zmq
import zmq.asyncio
from apache_iggy import Partitioning, SendMessage

from common import CLEARED, DEFAULT_URL, KV_EVENTS, PARTITIONS, REMOVED, STORED, STREAM, connect, ensure_topics, msg, part


def to_events(payload: bytes):
    """Yield (kind, block_hashes, parent) from one engine EventBatch."""
    batch = msgpack.unpackb(payload, raw=False, strict_map_key=False)
    events = batch[1] if isinstance(batch, (list, tuple)) else batch.get("events", [])
    for ev in events:
        tag = ev[0] if isinstance(ev, (list, tuple)) else ev.get("type")
        if tag == "BlockStored":
            yield STORED, list(ev[1]), ev[2]
        elif tag == "BlockRemoved":
            yield REMOVED, list(ev[1]), None
        elif tag == "AllBlocksCleared":
            yield CLEARED, [], None


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy, KV_EVENTS)
    producer = await iggy.producer(STREAM, KV_EVENTS, topic_partitions_count=PARTITIONS[KV_EVENTS])
    pid = part(a.deployment, PARTITIONS[KV_EVENTS])
    ctx = zmq.asyncio.Context()
    socks = []
    for ep in a.endpoint:
        s = ctx.socket(zmq.SUB)
        s.setsockopt(zmq.SUBSCRIBE, a.topic.encode())
        s.connect(ep)
        socks.append(s)
    seq, src_last, src_gaps = defaultdict(int), {}, defaultdict(int)

    async def publish(r, kind, blocks, parent):
        seq[r] += 1
        await producer.send_with_partitioning([SendMessage(msg(r=r, q=seq[r], e=kind, b=blocks, p=parent))],
                                              Partitioning.partition_id(pid))

    async def follow(r, s):
        while True:
            frames = await s.recv_multipart()
            if len(frames) < 3:
                continue
            n = int.from_bytes(frames[1], "big")
            if r in src_last and n > src_last[r] + 1:
                src_gaps[r] += n - src_last[r] - 1     # batches the engine sent that this socket never got
                print(f"[bridge] replica {r}: {n - src_last[r] - 1} event batches missed on PUB/SUB "
                      f"({src_gaps[r]} so far)", flush=True)
            src_last[r] = n
            try:
                for kind, blocks, parent in to_events(frames[2]):
                    await publish(r, kind, blocks, parent)
            except (ValueError, TypeError, IndexError, KeyError, msgpack.ExtraData):
                continue                               # not a batch this decoder understands

    async with producer:
        await asyncio.gather(*(follow(r, s) for r, s in enumerate(socks)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", action="append", required=True,
                   help="one replica's KV event publisher, e.g. tcp://10.0.3.17:5557; repeat for each replica")
    p.add_argument("--topic", default="", help="ZMQ topic prefix your engines publish under ('' = all)")
    p.add_argument("--deployment", default="qwen3-coder-480b", help="the model deployment; its partition key")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
