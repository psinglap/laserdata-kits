"""Shared settings for the Expedia Group bandit decision lane: every arm a Sampler picks for a traveler (which hero
image, layout or module order the traveler is shown), keyed by session, into every Sampler's consistency cache in
milliseconds, on Apache Iggy.

A message on the lane is one decision, the resulting value, never a delta: the session, the bandit (here one
hero-image bandit per property), the arm shown, the Sampler that picked it, and the time it was picked. A decision
never changes inside a session: the first one for a session and bandit wins everywhere, so a replay or a repeat
cannot change what a traveler sees.
"""
import asyncio
import json
import random
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "adaptex"
DECISIONS = "bandit-decisions"   # the lane: one record per new decision, keyed by session
REQUESTS = "page-requests"       # travelers' page views, routed by the load balancer to a healthy Sampler
RESPONSES = "page-picks"         # each Sampler's picks for a page, back to the travelers, who check them
PODS = "sampler-health"          # each Sampler's health beat, read by the load balancer
CLICKSTREAM = "clickstream"      # a heavy, latency-tolerant neighbour on the same broker
PARTITIONS = {DECISIONS: 16, REQUESTS: 12, RESPONSES: 8, PODS: 1, CLICKSTREAM: 16}
RETENTION = {DECISIONS: IggyExpiry.ExpireDuration(timedelta(hours=2)),     # longer than any session
             PODS: IggyExpiry.ExpireDuration(timedelta(minutes=10))}
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once


def part_of(key: str, partitions: int = PARTITIONS[DECISIONS]) -> int:
    # The one partition function used everywhere: a session's decisions always land in one partition, in order.
    return zlib.crc32(key.encode()) % partitions


def property_id(i: int) -> str:
    return f"H{100000 + i}"          # illustrative property ids


def arms_of(prop: str) -> list:
    """The hero-image bandit for one property: up to 10 candidate images, each with its clicks and impressions so
    far. Deterministic per property, so every Sampler holds the same parameters (the Bandit Store's copy)."""
    r = random.Random(zlib.crc32(prop.encode()))
    out = []
    for _ in range(r.randint(3, 10)):
        imp = r.randint(40, 900)                     # a bandit in its learning phase: few impressions per image
        out.append((1 + int(imp * r.uniform(0.02, 0.09)), imp))
    return [(c, i - c) for c, i in out]              # Beta(1 + clicks, 1 + impressions - clicks), +1 below


def thompson(params: list, rng: random.Random) -> int:
    # Thompson Sampling on click-through rate, as the hero-image bandit does: one Beta draw per image, highest wins.
    best, arm = -1.0, 0
    for i, (a, b) in enumerate(params):
        v = rng.betavariate(a + 1, b + 1)
        if v > best:
            best, arm = v, i
    return arm


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
