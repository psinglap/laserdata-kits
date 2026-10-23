"""Shared settings for the Lyft geohash forecast lane: every frame of real-time demand and supply forecasts, per
geohash-6 cell, from the forecasting models to Rider Pricing and Driver Earnings in milliseconds, on Apache Iggy.

A frame is one forecast run for one region: for every geohash-6 cell, the demand and supply forecast for each
5-minute interval of the next hour (12 values each), the shape Lyft's forecasting post describes. A region's
frame travels as chunks of cells followed by a seal that says how many chunks the frame has. All of a region's
messages land in one partition, in order, so a reader that has the seal has the whole frame.

Region names and geohash prefixes here are illustrative.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "marketplace-forecasts"
FORECASTS = "geohash-forecasts"     # the lane: chunks and seals of every region's forecast frames, keyed by region
ANALYTICS = "analytics-events"      # client and service events bound for feature generation: a heavy neighbour
PARTITIONS = {FORECASTS: 16, ANALYTICS: 16}
RETENTION = {FORECASTS: IggyExpiry.ExpireDuration(timedelta(hours=1))}   # a restarted reader rebuilds from the log
HORIZONS = 12                       # 5-minute intervals over the next hour
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

B32 = "0123456789bcdefghjkmnpqrstuvwxyz"   # the geohash alphabet


def region_key(i: int) -> str:
    return f"R{i:03d}"


def region_prefix(i: int) -> str:
    # An illustrative 4-character geohash prefix per region; a cell is the prefix plus 2 characters (geohash-6).
    return "9q" + B32[(i // 32) % 32] + B32[i % 32]


def cell(prefix: str, j: int) -> str:
    return prefix + B32[(j // 32) % 32] + B32[j % 32]


def part_of(key: str, partitions: int = PARTITIONS[FORECASTS]) -> int:
    # The one partition function used everywhere: every message of one region lands in one partition, in order.
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
