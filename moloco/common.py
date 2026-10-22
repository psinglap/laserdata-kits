"""Shared settings for the Moloco Commerce Media item status lane: every stock-out, restock and kill-switch
block for an advertised item reaches every Decision API server in milliseconds, on Apache Iggy.

A message on the lane is the latest status of one ad item: whether it is in stock, its stock count, and
whether it is blocked (the Item Kill Switch, or the catalog's `blocked` field). It carries the resulting
status, never a delta, with a per-item sequence number (the lane's version of the catalog's `updated_time`:
the newest wins) and the time the change was made, so any Decision API server can start from any message.
"""
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "mcm"
STATUS = "item-status"               # the lane: one message per status change, keyed by item
USER_EVENTS = "user-events"          # a heavy, latency-tolerant shopper event stream on the same broker
PARTITIONS = {STATUS: 8, USER_EVENTS: 16}
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=12))   # the kill switch blocks for 12 h; enough to rebuild and replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: no delayed-ACK stalls on small writes

# Catalog categories of the retailer's items in live campaigns; the names only need to be stable IDs.
CATEGORIES = ["electronics", "grocery", "home", "beauty", "apparel", "toys", "sports", "health",
              "baby", "pets", "office", "garden", "auto", "books", "appliances", "jewelry"]

# A message's step: what the change did to the item's ad eligibility (in stock and not blocked).
OUT, NONE, BACK = -1, 0, 1


def item(cat: str, n: int) -> str:
    # An item stands for whatever your catalog keys ads on: item_id, or item_id and seller_id with MSPI.
    # The kit only needs a stable ID, and the category first so a flash sale can pick one category.
    return f"{cat}|itm-{n:07d}"


def items(per_cat: int, cats: int = len(CATEGORIES)) -> list:
    return [item(c, n) for c in CATEGORIES[:cats] for n in range(per_cat)]


def part(key: str) -> int:
    # The same function in every script, so an item's changes always land on one partition, in order.
    return zlib.crc32(key.encode()) % PARTITIONS[STATUS]


def eligible(stock: int, blocked: str) -> bool:
    return stock > 0 and not blocked


def msg(k: str, seq: int, stock: int, blocked: str = "", step: int = NONE, waited_ns: int = 0) -> bytes:
    # blocked: "" or the reason (kill switch, recall, fraud report). Created time = now, minus how long the
    # change has already waited for its write.
    return json.dumps({"k": k, "q": seq, "n": stock, "b": blocked, "s": step,
                       "ns": time.time_ns() - waited_ns}, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topic(c: IggyClient, topic: str):
    """Create the stream and topic if they are missing, so any script can start first."""
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    if await c.get_topic(STREAM, topic) is None:
        try:
            await c.create_topic(STREAM, topic, PARTITIONS[topic], message_expiry=RETENTION)
        except RuntimeError:
            pass


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
