"""Shared settings for the Imbi notifications lane kit: deployment and mention notifications from Imbi services
reach slackbot and email in milliseconds, each posted once, and a reader whose position stops moving is flagged
within seconds, on Apache Iggy.

Names follow Imbi's ADR 0019 conventions: a stream per subject, topics as that subject's own dimension. The
`notifications` stream and its `deployment` and `mention` topics are the ones the ADR names for later; `events` and
`operations_log` are streams Imbi already publishes to (the ClickHouse sink path), used here only as the neighbour.
"""
import asyncio
import gc
import json
import time
import zlib

NOTIFY = "notifications"            # the lane: what Imbi services react to (ADR 0019 §1, "future")
DEPLOYMENT = "deployment"           # a deployment finished or was rolled back, keyed by project
MENTION = "mention"                 # a user was mentioned in a comment, keyed by user
LANE = (DEPLOYMENT, MENTION)

PROGRESS = "notification_readers"   # each reader's own position, once a second, read by watchdog.py
POSITIONS = "positions"

ANALYTICS = {"events": "gateway", "operations_log": "deployments"}   # existing ClickHouse-bound streams

DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"    # Nagle off: small writes go out at once
PARTITIONS = {DEPLOYMENT: 4, MENTION: 4, POSITIONS: 1, "gateway": 1, "deployments": 1}

ENVIRONMENTS = ("testing", "staging", "production")
ENTRY_TYPES = ("Deployed", "Deployed", "Deployed", "Deployed", "Rolled Back")   # operations_log entry_type values


def project(i: int) -> str:
    return f"project-{i:04d}"


def user(i: int) -> str:
    return f"user-{i:04d}"


def part(key: str, partitions: int) -> int:
    # The same function in every script, so one project's (or one user's) notifications stay in order.
    return zlib.crc32(key.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def ensure(client, stream: str, *topics: str):
    """Create the stream and topics if they are missing, so any script can start first."""
    if await client.get_stream(stream) is None:
        try:
            await client.create_stream(stream)
        except RuntimeError:
            pass                                    # created by another script in the meantime
    for t in topics:
        if await client.get_topic(stream, t) is None:
            try:
                await client.create_topic(stream, t, PARTITIONS[t])
            except RuntimeError:
                pass
    while not all([await client.get_topic(stream, t) for t in topics]):
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
    """Longest garbage-collection pause and longest event-loop stall since the last read, in ms, printed next to
    the latency so a slow second can be traced to this process (or ruled out)."""

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
