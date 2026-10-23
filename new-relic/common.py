"""Shared settings for the time-wave watermark lane kit: every time-wave generation your emitters send into a
cell, and every tally of its synthetic points reaching the end of that cell's pipeline, keyed by cell and source
pipeline, into the watermark service that alert evaluators ask before they close a window, in milliseconds,
on Apache Iggy."""
import asyncio
import gc
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyExpiry

STREAM = "time-waves"
LANE = "wave-lane"               # the lane: a record per generation emitted into a cell's pipeline, and tallies of
                                  # the Kafka partitions whose synthetic point for that generation reached the end,
                                  # both keyed by cell/pipeline
WATERMARKS = "cell-watermarks"   # the watermark service's output: complete-through generation and behind flags
DRAIN = "backlog-drain"          # a cell's backlog replayed after it recovers: big, read by nobody here (the neighbour)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {LANE: 16, WATERMARKS: 1, DRAIN: 16}
RETENTION = {LANE: IggyExpiry.ExpireDuration(timedelta(minutes=15)),   # enough to rebuild every watermark
             WATERMARKS: IggyExpiry.ExpireDuration(timedelta(hours=1)),
             DRAIN: IggyExpiry.ExpireDuration(timedelta(hours=1))}
PIPELINES = ("metrics", "events", "logs", "spans")   # source pipelines; measurements are scoped per pipeline and cell


def cell(i: int) -> str:
    return f"cell-{i:02d}"        # illustrative; yours come from the cell registry


def wave_key(c: str, pipeline: str) -> str:
    return f"{c}/{pipeline}"      # one cell's source pipeline: its waves and tallies stay in order on one partition


def part(key: str, partitions: int) -> int:
    # One Iggy partition per key, the same function in every script.
    return zlib.crc32(key.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


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
