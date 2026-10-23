"""Shared settings for the customer aggregate lane kit: each customer's loan history, rolled up by the lending
system of record, reaches Affirm's online feature store before the next checkout decision reads it, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "lending"
AGGREGATES = "user-aggregates"      # the lane: one user-level aggregate per loan event, keyed by customer
CHECKOUTS = "checkout-reads"        # the decision-time feature reads, with the system of record's counter, by customer
BACKFILL = "aggregate-backfill"     # a heavy, latency-tolerant history backfill on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {AGGREGATES: 16, CHECKOUTS: 16, BACKFILL: 32}

# "Precompute First" (Affirm Tech Blog, Oct 2026): "The pipeline targets under five minutes from loan event to
# updated feature value." Checkouts this soon after a loan event are the ones a slow pipeline sends to recompute.
RECENT_S = 300.0

# The five outcomes Affirm's post names for "is this newer than what I stored?", one pure function called from
# both the write path (incoming event vs stored value) and the read path (stored value vs the system of record).
FIRST, NEWER, OLDER, DUPLICATE, SCHEMA_UP = "first", "newer", "older", "duplicate", "schema_up"


def compare(stored, incoming):
    """stored and incoming are (counter, schema) or None. Returns one of the five outcomes."""
    if stored is None:
        return FIRST
    if incoming[1] > stored[1]:
        return SCHEMA_UP                    # a new schema version: take it, even at the same counter
    if incoming[1] < stored[1]:
        return OLDER                        # written under an older schema: never replaces a newer one
    if incoming[0] > stored[0]:
        return NEWER
    return DUPLICATE if incoming[0] == stored[0] else OLDER


def customer_id(n: int) -> str:
    return f"U{n:09d}"                      # illustrative customer IDs


def part(key: str, partitions: int) -> int:
    # One partition per customer, the same function in every script, so one customer's aggregates stay in order.
    return zlib.crc32(key.encode()) % partitions


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
