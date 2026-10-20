"""Shared settings for the failed-writes lane kit: the capture-and-retry path of the Event Publisher.

When a publish to the main MSK topics fails (a Kafka timeout during a traffic spike, or MSK
unavailable), the publishing client writes the event to a failed-writes topic on a separate
cluster. A failed-writes processor (the retrier) reads it back and re-publishes it to MSK. Events
that keep failing go to failed-writes-dlq, kept 7 days. This kit puts that separate cluster on
Apache Iggy: one topic for failed writes, one for the DLQ, keyed by profile so one profile's
events stay in order.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "event-publisher"
FAILED = "failed-writes"                                  # captured events waiting to be re-published
DLQ = "failed-writes-dlq"                                 # events that kept failing: alert, investigate
REPLAY = "s3-replay"                                      # bulk replay of events recovered from S3 (s3_replay.py)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
FAILED_RETENTION = IggyExpiry.ExpireDuration(timedelta(days=1))
DLQ_RETENTION = IggyExpiry.ExpireDuration(timedelta(days=7))      # "retained for 7 days" (Klaviyo, Oct 2025)

# Priority lanes and their processing SLO in seconds. P10 (real-time notifications, 5 s) and the
# 20-minute analytics bound are Klaviyo's published numbers; giving P30 the 20 minutes and P20
# 60 seconds is this kit's assumption. Shares of events per lane are illustrative.
PRIORITIES = {"P10": 5.0, "P20": 60.0, "P30": 1200.0}
SHARES = (("P10", 20), ("P20", 30), ("P30", 50))

# Events named after the customer interactions Klaviyo's event posts list, by lane (illustrative).
EVENTS = {
    "P10": ("Started Checkout", "Placed Order", "Added to Cart"),
    "P20": ("Viewed Product", "Active on Site", "Subscribed to List"),
    "P30": ("Opened Email", "Clicked Email", "Received Email"),
}


def part(profile: str, partitions: int) -> int:
    # Keyed by profile, so one profile's events stay in order. Same function in every script.
    return zlib.crc32(profile.encode()) % partitions


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


async def ensure_topic(c: IggyClient, stream: str, topic: str, partitions: int, expiry=FAILED_RETENTION):
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
