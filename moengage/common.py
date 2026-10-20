"""Shared settings for the Smart Trigger lane kit: one Apache Iggy topic per workspace.

Every workspace (one brand's app or site) gets its own topic in the smart-triggers stream.
Its trigger events (the events that live Smart Trigger campaigns and Flows wait on) go there,
keyed by user, so the trigger evaluator can read each workspace at its own rate and a burst
from one brand never sits in front of another brand's events.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "smart-triggers"
FIRES = "trigger-fires"              # campaign sends the evaluator decided on, for the channel senders
FIRES_PARTITIONS = 8
INGEST_STREAM, UNIFIED = "ingestion", "unified-events"   # the bulk ingestion firehose, next door
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090"
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=6))  # enough to replay a trigger window, not a data lake

# Trigger events and the share of all events each one is. The rest of the SDK and API traffic
# (page views, property updates, impressions) stays on the ingestion pipeline only.
EVENTS = (("Product Viewed", 55), ("App Opened", 20), ("Added To Cart", 15),
          ("OTP Requested", 4), ("Purchase Completed", 6))

# Live Smart Trigger campaigns, all sent "Immediately": trigger event -> channel.
CAMPAIGNS = {"Added To Cart": "push", "OTP Requested": "sms", "Purchase Completed": "whatsapp"}


def ws_name(i: int) -> str:
    return f"ws-{i + 1:04d}"


def weights(n: int) -> list:
    """Workspace sizes: a few large brands and a long tail (Zipf-like), summing to 1."""
    w = [1 / (i + 1) ** 0.9 for i in range(n)]
    s = sum(w)
    return [x / s for x in w]


def partitions_for(i: int, n: int) -> int:
    # The largest 5% of workspaces get 4 partitions, so one brand's users spread over several
    # readers; everyone else gets 1. Same function in every script.
    return 4 if i < max(1, n // 20) else 1


def part(user: str, partitions: int) -> int:
    # One partition per user within a workspace, so a user's events stay in order.
    return zlib.crc32(user.encode()) % partitions


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


async def ensure_topic(c: IggyClient, stream: str, topic: str, partitions: int, expiry=RETENTION):
    """Create the stream and topic if they are missing, so any script can start first."""
    if await c.get_stream(stream) is None:
        try:
            await c.create_stream(stream)
        except RuntimeError:
            pass                                    # created by another process in the meantime
    if await c.get_topic(stream, topic) is None:
        try:
            await c.create_topic(stream, topic, partitions, message_expiry=expiry)
        except RuntimeError:
            pass
    while not await c.get_topic(stream, topic):
        await asyncio.sleep(0.2)


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
