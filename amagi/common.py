"""Shared settings for the quartile tracker lane kit: every ad segment a THUNDERSTORM stitcher serves to a viewer,
keyed by session, reaches the tracker that owns that session in milliseconds, and the tracker fires each quartile
beacon (start, firstQuartile, midpoint, thirdQuartile, complete) once, even when a segment event arrives twice or
the tracker restarts, on Apache Iggy.

A message on the lane is one ad segment served: the session, the channel, the impression (session, break and
slot), the creative, the segment's index, the ad's segment count, the ad's length and segment length, and when
the session's ad began. That is enough for the tracker to know which quartile points the segment contains, so a
message can be handled on its own and a repeat of it changes nothing.
"""
import asyncio
import gc
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "thunderstorm"
SEGMENTS = "ad-segments"        # the lane: one message per ad segment served, keyed by session
FIRED = "beacons-fired"         # the tracker's record of every beacon it fired, keyed by session (same partitions)
SESSIONS = "session-records"    # a heavy, latency-tolerant neighbour: periodic session records for analytics
PARTITIONS = {SEGMENTS: 16, FIRED: 16, SESSIONS: 16}
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=2))   # covers any tracker restart and a reporting replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

# The VAST tracking events a server-side tracker fires for one ad, in order.
QUARTILES = ["start", "firstQuartile", "midpoint", "thirdQuartile", "complete"]


def session(i: int) -> str:
    return f"sess-{i:07d}"


def channel(i: int) -> str:
    return f"fast-{i:03d}"        # illustrative FAST channel IDs


def part(key: str, partitions: int) -> int:
    # The same function in every script, so a session's segments and beacons always land on one partition, in order.
    return zlib.crc32(key.encode()) % partitions


def due_quartiles(g: int, n: int, ad_ms: int, seg_ms: int) -> list:
    """The quartile points inside segment g of an ad with n segments: point k sits at k/4 of the ad, so it is in
    the segment that covers that moment (the last segment holds 'complete'). The tracker fires a quartile's
    beacon when the player fetches that segment."""
    return [k for k in range(5) if min(int(k * ad_ms / 4 // seg_ms), n - 1) == g]


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


class Stalls:
    """Longest garbage-collection pause and longest event-loop stall since the last read, in ms. Printed next to
    the latency, so a slow second can be traced to this process (or ruled out) rather than guessed at."""

    def __init__(self):
        self.gc = self.loop = 0.0
        self._t = 0.0
        gc.callbacks.append(self._cb)

    def _cb(self, phase, info):
        if phase == "start":
            self._t = time.perf_counter()
        else:
            self.gc = max(self.gc, (time.perf_counter() - self._t) * 1000)

    async def watch(self):
        while True:
            t = time.perf_counter()
            await asyncio.sleep(0.005)
            self.loop = max(self.loop, (time.perf_counter() - t - 0.005) * 1000)

    def read(self):
        out = f"stall gc {self.gc:.0f}ms loop {self.loop:.0f}ms"
        self.gc = self.loop = 0.0
        return out
