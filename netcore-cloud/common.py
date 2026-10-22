"""Shared settings for the delivery status lane kit: one Apache Iggy topic per status type.

Every status webhook a channel provider sends back (Sent, Delivered, Read) goes to its own
topic in the campaign-status stream, keyed by message ID. Each status has its own reporter,
reading its own topic from its own offsets at its own rate, so a backlog of Read webhooks
waits on the Read topic and the Delivered counts on campaign reports stay current.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "campaign-status"
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))   # a day of webhooks: enough to rebuild a report

# status -> (topic, partitions). Delivered gets the most partitions: it is the signal reports wait on.
STATUSES = {
    "sent": ("status-sent", 4),
    "delivered": ("status-delivered", 8),
    "read": ("status-read", 8),
}


def campaign_name(i: int) -> str:
    return f"cmp-{i + 1:04d}"


def weights(n: int) -> list:
    """Campaign sizes: a few national broadcasts and a long tail (Zipf-like), summing to 1."""
    w = [1 / (i + 1) ** 0.9 for i in range(n)]
    s = sum(w)
    return [x / s for x in w]


def part(message_id: str, partitions: int) -> int:
    # Same partition function everywhere: every webhook for one message lands on the same partition
    # of its status topic, so a provider's retry of a webhook meets the original in one reader.
    return zlib.crc32(message_id.encode()) % partitions


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


class Seen:
    """Message IDs already counted, kept for two windows: provider retries arrive within minutes."""

    def __init__(self, window_s: float = 600):
        self.cur, self.old, self.window, self.t = set(), set(), window_s, time.monotonic()

    def add(self, k: str) -> bool:
        """True if k is new; False for a repeat."""
        if time.monotonic() - self.t > self.window:
            self.old, self.cur, self.t = self.cur, set(), time.monotonic()
        if k in self.cur or k in self.old:
            return False
        self.cur.add(k)
        return True


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
