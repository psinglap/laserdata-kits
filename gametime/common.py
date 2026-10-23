"""Shared settings for the Gametime Zone Deals pool lane: every listing change (tickets left, price, seller floor)
keyed by event and zone, into every Zone Deals seat picker in milliseconds, on Apache Iggy.

A message on the lane is the resulting state of one listing after a change, never a delta: its event, its zone,
tickets left, price per ticket, the seller's floor, whether the seller opted into automatic pricing, how many Zone
Deals tickets checkout has already processed against it, a per-listing sequence number, and the time the change
was created.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "marketplace"
LISTINGS = "zone-listings"       # the lane: one record per listing change, partitioned by event:zone, resulting state
ORDERS = "zone-orders"           # fans buying a Zone Deal, keyed by event:zone (one picker owns a zone's orders)
PICKS = "zone-picks"             # each picker's seat pick, back to checkout, which checks it against the listing
APP_EVENTS = "app-events"        # a heavy, latency-tolerant neighbour on the same broker
PARTITIONS = {LISTINGS: 16, ORDERS: 8, PICKS: 8, APP_EVENTS: 16}
RETENTION = {LISTINGS: IggyExpiry.ExpireDuration(timedelta(hours=6))}   # a new picker rebuilds every pool from the log
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

# Illustrative zone names; a venue map defines its own.
ZONES = ("LOWER-SIDELINE", "LOWER-CORNER", "LOWER-END", "UPPER-SIDELINE", "UPPER-CORNER", "UPPER-END",
         "CLUB", "FLOOR")


def zone_key(event: int, zone: str) -> str:
    return f"E{event:05d}:{zone}"


def part_of(key: str, partitions: int = PARTITIONS[LISTINGS]) -> int:
    # The one partition function used everywhere: every listing in one event's zone lands in one partition, in
    # order, so a zone's pool is never applied out of order across its listings.
    return zlib.crc32(key.encode()) % partitions


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
