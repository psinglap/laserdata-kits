"""Shared settings for the Freshworks agent state lane: every agent's availability and load across Freshdesk,
Freshchat and Freshcaller, keyed by agent, into every Omniroute routing node in milliseconds, on Apache Iggy.

A message on the lane is the resulting state of one agent after a change, never a delta: the agent's group,
status (available, away, offline), open tickets and ticket capacity, active chats and chat capacity, whether the
agent is on a call, how many assignments the agent has received, a per-agent sequence number, the product that
made the change, and the time the change was created.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "omniroute"
STATE = "agent-state"            # the lane: one record per agent change, keyed by agent, resulting state
INQUIRIES = "inquiries"          # new tickets and chats waiting for an agent, keyed by group
ASSIGNMENTS = "assignments"      # each routing node's pick, back to the products, which check it against the agent
PRODUCT = "product-events"       # a heavy, latency-tolerant neighbour: product events bound for the data platform
PARTITIONS = {STATE: 16, INQUIRIES: 8, ASSIGNMENTS: 8, PRODUCT: 16}
RETENTION = {STATE: IggyExpiry.ExpireDuration(timedelta(hours=1))}   # agents re-send state every few seconds
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once


def part_of(key: str, partitions: int = PARTITIONS[STATE]) -> int:
    # The one partition function used everywhere: an agent's changes always land in one partition, in order.
    return zlib.crc32(key.encode()) % partitions


def group_id(i: int) -> str:
    return f"grp-{i:04d}"         # illustrative: an agent group in one Freshworks account


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topics(c: IggyClient, *topics):
    """Create the stream and topics if they are missing, so any script can start first."""
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    for t in topics:
        if await c.get_topic(STREAM, t) is None:
            try:
                await c.create_topic(STREAM, t, PARTITIONS[t], message_expiry=RETENTION.get(t))
            except RuntimeError:
                pass
    while not all([await c.get_topic(STREAM, t) for t in topics]):
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
