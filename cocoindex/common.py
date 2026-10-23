"""Shared settings for the Apache Iggy live-context kit for CocoIndex: pull-request review threads change on an Iggy
topic, and a CocoIndex app keeps a LanceDB table of every open thread current, through CocoIndex's own Iggy connector
(`cocoindex.connectors.iggy.topic_as_map`).

Every message carries the thread's whole current state and a per-thread `seq`, never a delta, so a replayed message
can only rewrite the same row. A resolved thread is a deletion message in the shape CocoIndex's own Iggy examples
use: `{"_deleted": "<thread_id>"}` (Iggy has no Kafka-style tombstones).
"""
import asyncio
import json
import os
import time
from datetime import timedelta

from apache_iggy import IggyExpiry

STREAM = "code-review"
EVENTS = "review-events"        # what the review service writes: one message per thread change
SHADOW = "shadow-events"        # what kafka_shadow.py writes: a Kafka topic you already index, mirrored
# One partition: CocoIndex's Iggy source reads one partition per source, and with apache-iggy 0.9 a consumer-group
# member is assigned every partition of a topic, so multi-partition topics need a connector change (see README).
PARTITIONS = 1
RETENTION = IggyExpiry.ExpireDuration(timedelta(days=7))   # a rebuilt index replays the last week from Iggy
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
LANCEDB_URI = os.environ.get("LANCEDB_URI", "./lancedb_data")
TABLE = "review_context"
HEARTBEAT = ".index-heartbeat"  # index_app.py touches it every 100 ms; context_check.py uses it to see a crash

# Illustrative shape: an engineering org's open review threads across its repositories.
REPOS = ["api-gateway", "billing", "search", "web-app", "mobile-app", "infra", "data-pipelines", "auth",
         "notifications", "ml-platform", "payments", "docs"]
PATHS = ["src/handlers/{}.py", "src/models/{}.rs", "lib/{}/client.ts", "pkg/{}/server.go", "tests/test_{}.py"]
TOPICS = ["retry", "session", "cache", "invoice", "ranking", "token", "webhook", "schema", "quota", "index"]
COMMENTS = ["This retry has no upper bound; cap it or add jitter.", "Can we keep the old field until clients move?",
            "Nit: name this after what it returns.", "This lock is held across an await.",
            "Missing a test for the empty case.", "Looks good once CI is green.", "Why not reuse the existing helper?",
            "This changes the public API; needs a migration note.", "Fixed in the latest push.",
            "The N+1 query is back here."]


def key_of(message) -> str | None:
    """Application key for CocoIndex's topic_as_map: the thread id, or the id a deletion names."""
    d = json.loads(message.payload())
    return d.get("thread_id") or d.get("_deleted")


def is_deletion(message) -> bool:
    return b'"_deleted"' in message.payload()


async def ensure_topic(client, topic=EVENTS):
    """Create the stream and topic if they are missing, so any script can start first. CocoIndex never creates or
    drops Iggy streams and topics; they are user-managed."""
    if await client.get_stream(STREAM) is None:
        try:
            await client.create_stream(STREAM)
        except RuntimeError:
            pass                                    # created by another script in the meantime
    if await client.get_topic(STREAM, topic) is None:
        try:
            await client.create_topic(STREAM, topic, PARTITIONS, message_expiry=RETENTION)
        except RuntimeError:
            pass
    while not await client.get_topic(STREAM, topic):
        await asyncio.sleep(0.5)


async def topic_end(client, topic=EVENTS) -> int:
    """Next offset to be written on partition 0 (0 for an empty topic)."""
    t = await client.get_topic(STREAM, topic)
    p = t.partitions[0]
    return p.current_offset + 1 if p.messages_count else 0


def heartbeat_age(path=HEARTBEAT) -> float:
    try:
        return time.time() - os.stat(path).st_mtime
    except FileNotFoundError:
        return 1e9


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
