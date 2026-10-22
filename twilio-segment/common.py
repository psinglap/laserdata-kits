"""Shared settings for the destination delivery alert lane kit: every delivery attempt's outcome, keyed by
workspace and destination, reaches the alert check in milliseconds, and each delivery-rate alert and recovery
reaches the alert service within seconds of the first failures, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "segment-delivery"
OUTCOMES = "delivery-outcomes"   # the lane: one record per delivery attempt, keyed by workspace:destination
ALERTS = "delivery-alerts"       # the alert check's ALERT and RESOLVED records, read by the alert service
ARCHIVE = "event-archive"        # a heavy, latency-tolerant stream (raw event payloads for the archive) on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {OUTCOMES: 16, ALERTS: 1, ARCHIVE: 32}

# Destination types a workspace connects (illustrative; your destination catalog IDs go here).
DESTINATIONS = ("crm", "ads-audience", "email", "analytics", "webhook", "support", "data-lake", "push")

ALERT = "alert"                  # the successful delivery rate fell below the workspace's threshold
RESOLVED = "resolved"            # back above it

# Why an attempt fails in the planted incidents: (HTTP status, share of attempts that fail, what happened).
INCIDENTS = (("401", 1.0, "credentials revoked"), ("429", 0.5, "rate limited by the partner"),
             ("503", 0.8, "partner API down"))


def key(workspace: str, destination: str) -> str:
    return f"{workspace}:{destination}"


def workspace_id(i: int) -> str:
    return f"ws-{i:05d}"


def threshold(k: str) -> float:
    """The successful delivery rate the workspace set on its alert for this destination (stands in for your
    alert settings): 99%, 98% or 95%."""
    return (0.99, 0.98, 0.95)[zlib.crc32(k.encode()) % 3]


def part(k: str, partitions: int) -> int:
    # One partition per workspace:destination, the same function in every script, so a key's outcomes stay in order.
    return zlib.crc32(k.encode()) % partitions


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
                await client.create_topic(STREAM, t, PARTITIONS[t])
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
