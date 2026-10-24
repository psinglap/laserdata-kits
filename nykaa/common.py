"""Shared settings for the Nykaa ad counter lane kit: every sponsored ad impression and click reaches every ad
selector in milliseconds, before that shopper's next page asks for ads, on Apache Iggy.

A message on the lane is one ad event, as the event collector received it: the shopper, what it counts (a
campaign for an impression, the clicked campaign for a click), the counting window, and the collector's running
count for that shopper and key in the window (the resulting count, never +1), with a per-(collector, shopper)
sequence number and the time the collector received it. Any ad selector can start from any message, and a
replayed or repeated one changes nothing.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "nykaa-ads"
EVENTS = "ad-events"                 # the lane: sponsored ad impressions and clicks, keyed by shopper
REQUESTS = "ad-requests"             # ad requests from page loads, one partition per ad selector
CLICKSTREAM = "clickstream"          # the heavy, latency-tolerant clickstream feed on the same broker
PARTITIONS = {EVENTS: 8, REQUESTS: 8, CLICKSTREAM: 16}
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=48))   # two daily windows, for rebuilds and replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
BRANDS = 25                          # campaign i is run by brand i % BRANDS


def shopper(i: int) -> str:
    return f"s-{i:08d}"


def campaign(i: int) -> str:
    return f"cmp-{i:03d}"


def clicked(c: str) -> str:
    # The key a click counts under: its own key, so clicks and impressions of one campaign never mix.
    return "clk:" + c


def brand_of(c: str) -> str:
    return f"brand-{int(c[4:]) % BRANDS:02d}"


def part(key: str, partitions: int) -> int:
    # The same function in every script, so a shopper's events always land on one partition, in order.
    return zlib.crc32(key.encode()) % partitions


def window(window_s: float, now: float | None = None) -> int:
    # The counting window (a day in production; --window-s compresses it), derived from the clock by every script.
    return int((time.time() if now is None else now) // window_s)


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
                await c.create_topic(STREAM, t, PARTITIONS[t], message_expiry=RETENTION)
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
