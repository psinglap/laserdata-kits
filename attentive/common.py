"""Shared settings for the subscriber decision lane kit.

Shopping events (the user-intent events the decision engine reads to decide when to text a
subscriber) go to one Apache Iggy topic, keyed by subscriber ID: every event of one subscriber
lands on the same partition, in order. Each decision worker owns a fixed set of partitions and
reads them directly, so one subscriber's events are only ever decided by one worker, one at a
time, with no lock. The decisions go to a send-intents topic, keyed the same way, which the send
service (here send_audit.py) reads.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "decisions"
EVENTS_TOPIC = "subscriber-events"                 # shopping events, keyed by subscriber
SENDS_TOPIC = "send-intents"                       # the decision engine's output, keyed by subscriber
CAMPAIGN_STREAM = "campaigns"                      # the scheduled-campaign sends next door (bfcm_flood.py)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))          # a day of decisions to replay or audit

# Shopping events from the onsite tag and the brand's store, with each one's share (illustrative).
EVENTS = (("product_view", 62), ("add_to_cart", 18), ("checkout_started", 8), ("purchase", 4),
          ("sms_click", 5), ("email_click", 3))
INTENT = {"add_to_cart", "checkout_started"}       # events that can trigger a text


def subscriber_id(n: int) -> str:
    return f"s{n:09d}"


def brand_of(sub: str, brands: int) -> str:
    return f"brand-{zlib.crc32(sub.encode()) % brands + 1:04d}"


def part(sub: str, partitions: int) -> int:
    # The same function everywhere: one subscriber, one partition, one owner.
    return zlib.crc32(sub.encode()) % partitions


def owner(pid: int, workers: int) -> int:
    # Static ownership: partition p belongs to worker p % workers. No lock, no lease, no rebalance.
    return pid % workers


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
