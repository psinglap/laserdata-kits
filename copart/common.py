"""Shared settings for the Copart VB3 live lot lane: every accepted live bid, countdown restart and sale, keyed by
sale lane, into every bidder-facing edge node in milliseconds, on Apache Iggy.

A message on the lane is the resulting state of one sale lane after a change, never a delta: the lot on the block,
whether it is open or sold, the high bid, the high bidder's region (the flag on the bidding screen), the lot's
estimated retail value, when the countdown runs out, the bid that made this state, a per-lane sequence number and
the time the change was created.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "vb3-live"
LOT_STATE = "lot-state"            # the lane: one record per change of a sale lane, resulting state, keyed by lane
BIDS = "live-bids"                 # members' live bids from the edge nodes, keyed by lane, judged by the auction core
MEMBER_ACTIVITY = "member-activity"   # searches, lot views, watchlist adds: a heavy neighbour on the same broker
PARTITIONS = {LOT_STATE: 16, BIDS: 8, MEMBER_ACTIVITY: 16}
RETENTION = {LOT_STATE: IggyExpiry.ExpireDuration(timedelta(hours=6))}   # a new edge rebuilds every lane from the log
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

# Illustrative regions for the bidder flags on the bidding screen; members bid from over 185 countries.
REGIONS = ("US", "CA", "MX", "GB", "DE", "UA", "PL", "GE", "NG", "AE", "BR", "SV")


def lane_key(yard: int, lane: int) -> str:
    return f"Y{yard:03d}-{chr(65 + lane)}"         # a yard's virtual sale lane, e.g. Y112-B (illustrative)


def increment(high: int) -> int:
    # Illustrative bid increments in USD; set them to VB3's own table.
    return 25 if high < 1000 else 50 if high < 5000 else 100


def part_of(key: str, partitions: int = PARTITIONS[LOT_STATE]) -> int:
    # The one partition function used everywhere: every change of one sale lane lands in one partition, in order.
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
