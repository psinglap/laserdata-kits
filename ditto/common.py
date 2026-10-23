"""Shared settings for the Ditto CDC Data Bridge lane kit: every Big Peer document change, as a Ditto CDC
event, reaches every downstream mirror in per-document order in milliseconds, on Apache Iggy."""
import asyncio
import json
import time
import zlib
from datetime import datetime, timezone

from apache_iggy import IggyClient

STREAM = "ditto-cdc"
CHANGES = "cabin-orders"         # the lane: one Data Bridge's events (collection cabin_orders), keyed by document _id
EXPORT = "bulk-export"           # a heavy, latency-tolerant stream on the same broker (historical export)
COLLECTION = "cabin_orders"
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
# 12 is Ditto's default Data Bridge partition count ("supports 1, 2, 3, 4, 6, or 12 parallel consumers").
PARTITIONS = {CHANGES: 12, EXPORT: 12}


def doc_key(_id) -> str:
    # Ditto _ids can be strings or objects; one stable string per document.
    return _id if isinstance(_id, str) else json.dumps(_id, sort_keys=True, separators=(",", ":"))


def part_of(key: str, partitions: int = PARTITIONS[CHANGES]) -> int:
    # The one partition function used everywhere: a document's changes always land in one partition, in order,
    # which is the guarantee Ditto CDC gives ("event ordering at the individual document level").
    return zlib.crc32(key.encode()) % partitions


def event_key(e: dict) -> str | None:
    """The document an event belongs to, or None for requeryRequired (it concerns the whole bridge)."""
    if e.get("type") != "documentChanged":
        return None
    c = e["change"]
    doc = c.get("newValue") or c.get("value") or c.get("oldValue")
    return doc_key(doc["_id"]) if doc else None


def cdc_now(ns: int) -> str:
    # cdcTimestamp as Ditto writes it: RFC 3339 with nanoseconds.
    s, n = divmod(ns, 1_000_000_000)
    return datetime.fromtimestamp(s, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{n:09d}Z"


def changed(txn: int, method: str, ns: int, old=None, new=None, value=None) -> dict:
    """A documentChanged event in Ditto's CDC shape, plus ns (when it was created) for timing."""
    if method == "upsert":
        change = {"method": method, "oldValue": old, "newValue": new}
    else:                                   # remove, evict, snapshot carry the full document as value
        change = {"method": method, "value": value}
    return {"txnId": txn, "version": 1, "cdcTimestamp": cdc_now(ns), "type": "documentChanged",
            "collection": COLLECTION, "change": change, "ns": ns}


def dump(d: dict) -> bytes:
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
                await c.create_topic(STREAM, t, PARTITIONS[t])
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


def now_ns() -> int:
    return time.time_ns()
