"""Shared settings for the Roblox server state lane: every game server's join-relevant state (players, capacity,
open or shutting down), keyed by server, into every matchmaking index in milliseconds, on Apache Iggy.

A message on the lane is the resulting state of one server after a change, never a delta: its place, its edge
data center, capacity, players in it, status (open, closing for an update, closed), how many placements it has
processed, a per-server sequence number, and the time the engine instance created the message.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "matchmaking"
STATE = "server-state"           # the lane: one record per server change, keyed by server, resulting state
JOINS = "join-requests"          # players pressing Play, keyed by place (one matchmaker owns a place's joins)
PLACEMENTS = "placements"        # each matchmaker's pick, back to the fleet, which checks it against the server
ANALYTICS = "analytics-events"   # a heavy, latency-tolerant neighbour on the same broker
PARTITIONS = {STATE: 16, JOINS: 8, PLACEMENTS: 8, ANALYTICS: 16}
RETENTION = {STATE: IggyExpiry.ExpireDuration(timedelta(hours=1))}   # heartbeats every few seconds: an hour holds every live server
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once


def part_of(key: str, partitions: int = PARTITIONS[STATE]) -> int:
    # The one partition function used everywhere: a server's changes always land in one partition, in order.
    return zlib.crc32(key.encode()) % partitions


def place_id(i: int) -> str:
    return f"place-{i:05d}"


def dc_id(i: int) -> str:
    return f"edge-{i + 1:02d}"       # illustrative names for the edge data centers that run game servers


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
