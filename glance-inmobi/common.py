"""Shared settings for the Glance ad-state lane kit: every impression and dismiss beacon from a lock screen
reaches every ad server in milliseconds, before that phone asks for its next prefetch refill, on Apache Iggy.

A message on the lane is one beacon, as the beacon collector received it: the user, what it counts (a campaign
for an impression, an advertiser for a dismiss), the cap window, and the collector's running total for that
user and key in the window (the resulting total, never +1), with a per-(collector, user) sequence number and
the time the collector received it. Any ad server can start from any message, and a replayed or repeated one
changes nothing.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "glance-ads"
SIGNALS = "user-ad-signals"          # the lane: impression and dismiss beacons, keyed by user
REFILLS = "prefetch-refills"         # refill requests from phones, one partition per ad server
INTERACTIONS = "interaction-events"  # a heavy, latency-tolerant content interaction feed on the same broker
PARTITIONS = {SIGNALS: 8, REFILLS: 8, INTERACTIONS: 16}
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=48))   # two daily cap windows, for rebuilds and replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
ADVERTISERS = 20                     # campaign i belongs to advertiser i % ADVERTISERS


def user(i: int) -> str:
    return f"u-{i:08d}"


def campaign(i: int) -> str:
    return f"cmp-{i:03d}"


def advertiser_of(c: str) -> str:
    return f"adv-{int(c[4:]) % ADVERTISERS:02d}"


def part(key: str, partitions: int) -> int:
    # The same function in every script, so a user's beacons always land on one partition, in order.
    return zlib.crc32(key.encode()) % partitions


def cap_window(window_s: float, now: float | None = None) -> int:
    # The cap window (a day in production; --window-s compresses it), derived from the clock by every script.
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
