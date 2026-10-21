"""Shared settings for the live events lane kit: every captured event, keyed by project, on a lane that the
live view reads directly, so the node serving a viewer reads that viewer's project from the log in
milliseconds, on Apache Iggy."""
import asyncio
import gc
import json
import random
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyExpiry

STREAM = "posthog-live"
LANE = "live-events"             # one compact record per captured event, keyed by project token
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {LANE: 256}         # a project lives on one partition; a live node reads only its viewers' partitions
RETENTION = {LANE: IggyExpiry.ExpireDuration(timedelta(minutes=10))}   # Live shows "the last few minutes"
LAUNCH = "phc_launch_demo"       # the project that ships a launch in launch.py (its own token, its own writer)

PATHS = ["/", "/pricing", "/docs", "/blog", "/signup", "/login", "/changelog", "/careers", "/product", "/about"]
EVENTS = ["$pageview", "$pageview", "$pageview", "$autocapture", "$autocapture", "$pageleave", "$identify",
          "signed_up", "$feature_flag_called", "$exception"]


def token(i: int) -> str:
    # An illustrative project API token; yours come from capture's `token` header.
    return f"phc_{zlib.crc32(f'project-{i}'.encode()):08x}{i:06d}"


def part(key: str, partitions: int) -> int:
    # One Iggy partition per project, the same function in every script, so a project's events stay in order.
    return zlib.crc32(key.encode()) % partitions


def watched(projects: int, n: int, seed: int = 11) -> list:
    """The projects that have an open Live view at start: a mix of big and small ones, drawn the same way in
    every script."""
    return random.Random(seed).sample(range(projects), min(n, projects))


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def ensure_topics(client, *topics, partitions=None):
    """Create the stream and topics if they are missing, so any script can start first."""
    if await client.get_stream(STREAM) is None:
        try:
            await client.create_stream(STREAM)
        except RuntimeError:
            pass                                    # created by another script in the meantime
    for t in topics:
        if await client.get_topic(STREAM, t) is None:
            try:
                await client.create_topic(STREAM, t, (partitions or PARTITIONS)[t], message_expiry=RETENTION[t])
            except RuntimeError:
                pass
    while not all([await client.get_topic(STREAM, t) for t in topics]):
        await asyncio.sleep(0.5)
    return (await client.get_topic(STREAM, topics[0])).partitions_count


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
