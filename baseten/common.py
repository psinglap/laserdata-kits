"""Shared settings for the KV cache event lane kit: every block stored and every block removed on every model
replica reaches every KV cache-aware router in milliseconds, in order and without gaps, on Apache Iggy.

A message on the lane is one KV cache event from one replica: the blocks it just stored (with the parent block
they extend) or the blocks it just evicted. Each carries the replica's own event sequence and the time the
replica made the change, so a router can apply events in order, see a gap if one were ever missing, and start
from any point of the log.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "inference-stack"
KV_EVENTS = "kv-events"              # the lane: one message per stored or removed set of KV blocks, keyed by deployment
TELEMETRY = "inference-telemetry"    # a heavy, latency-tolerant stream on the same broker: per-request spans
PARTITIONS = {KV_EVENTS: 8, TELEMETRY: 32}   # kv-events: one partition per model deployment (8 here)
# Dynamo's durable KV event mode keeps prefix blocks in its event stream for one hour; the lane keeps the same.
RETENTION = {KV_EVENTS: IggyExpiry.ExpireDuration(timedelta(hours=1)),
             TELEMETRY: IggyExpiry.ExpireDuration(timedelta(hours=2))}
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

STORED, REMOVED, CLEARED = "stored", "removed", "cleared"   # CLEARED: a replica dropped its whole cache
# Tokens per KV block in this kit. Engines use smaller blocks (16 to 64 tokens) and send many per event; the kit
# uses coarse blocks so one Python process can play a whole deployment. Only the token arithmetic uses it.
BLOCK_TOKENS = 512


def replica(i: int) -> str:
    return f"replica-{i:02d}"


def part(deployment: str, partitions: int) -> int:
    # A deployment's events share one partition, the same function in every script: its routers read one
    # partition, and every replica's events stay in the order the replica made them.
    return zlib.crc32(deployment.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def connect(url: str) -> IggyClient:
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topics(client, *topics):
    """Create the stream and topics if they are missing, so any script can start first."""
    if await client.get_stream(STREAM) is None:
        try:
            await client.create_stream(STREAM)
        except RuntimeError:
            pass                                    # created by another script in the meantime
    for t in topics:
        if await client.get_topic(STREAM, t) is None:
            try:
                await client.create_topic(STREAM, t, PARTITIONS[t], message_expiry=RETENTION[t])
            except RuntimeError:
                pass
    while not all([await client.get_topic(STREAM, t) for t in topics]):
        await asyncio.sleep(0.5)


class Window:
    def __init__(self):
        self.samples = []

    def add(self, v):
        self.samples.append(v)

    def flush(self):
        s = sorted(self.samples) or [0.0]
        pick = lambda q: s[min(len(s) - 1, int(q * len(s)))]
        out = {"n": len(self.samples), "p50": pick(0.5), "p99": pick(0.99), "p999": pick(0.999), "max": s[-1]}
        self.samples = []
        return out
