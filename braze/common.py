"""Shared settings for the Currents connector lane kit: one Apache Iggy topic per Currents connector.

Every Currents connector (one customer app's export to one destination: a storage bucket, an
analytics partner, a customer data platform) gets its own topic in the currents stream. The
router writes each engagement event to the topic of every connector configured for that event
type, keyed by user ID. Each connector reads its own topic at its partner's pace and keeps its
own offsets, so a partner that is down or rate-limiting only ever delays its own connector.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "currents"
RAW_STREAM = "events"                                    # the event-type topics next door (firehose.py)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))          # a day of export to replay after an outage

# Engagement and behaviour events, named as in Braze's Kafka talk and Currents docs, with the share of
# exported events each one is (illustrative).
EVENTS = (("Push Notification Send", 30), ("Email Delivery", 20), ("In-App Message Impression", 15),
          ("Custom Event", 12), ("Email Open", 10), ("Push Notification Open", 8), ("Purchase", 3),
          ("Campaign Conversion", 2))

# Connector kinds and the event types each one is configured to export. Every app has a storage
# connector; every second app also exports to an analytics partner; every fifth to a CDP.
KINDS = {
    "storage": {e for e, _ in EVENTS},
    "analytics": {"Push Notification Open", "Email Open", "In-App Message Impression", "Custom Event",
                  "Purchase", "Campaign Conversion"},
    "cdp": {"Push Notification Send", "Push Notification Open", "Email Delivery", "Email Open",
            "Campaign Conversion"},
}


def app_name(i: int) -> str:
    return f"app-{i + 1:04d}"


def kinds_for(i: int) -> list:
    return ["storage"] + (["analytics"] if i % 2 == 0 else []) + (["cdp"] if i % 5 == 0 else [])


def weights(n: int) -> list:
    """App sizes: a few very large apps and a long tail (Zipf-like), summing to 1."""
    w = [1 / (i + 1) ** 0.9 for i in range(n)]
    s = sum(w)
    return [x / s for x in w]


def partitions_for(i: int, n: int) -> int:
    # The largest 5% of apps get 4 partitions per connector; everyone else 1. Same function in every script.
    return 4 if i < max(1, n // 20) else 1


def connectors(n: int) -> dict:
    """Every connector of the kit's n apps: topic name -> (app index, kind, partitions)."""
    return {f"{app_name(i)}-{k}": (i, k, partitions_for(i, n)) for i in range(n) for k in kinds_for(i)}


def shard_of(topic: str, shards: int) -> int:
    return zlib.crc32(topic.encode()) % shards


def part(user: str, partitions: int) -> int:
    # Keyed by user ID, as Braze keys its event topics: one user's events stay in order on one partition.
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
