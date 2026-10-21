"""Shared settings for the viewed-posts lane kit: the posts each user has just viewed reach the feed's
dedup service before that user's next feed request, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "feed-dedup"
VIEWED = "viewed-posts"         # the lane: each app flush of viewed post IDs, keyed by user
REQUESTS = "feed-requests"      # the user's next feed request, which must already exclude those posts
ENGAGEMENT = "engagement-events"  # a heavy, latency-tolerant stream on the same broker (likes, views for counters)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {VIEWED: 16, REQUESTS: 16, ENGAGEMENT: 32}


def user_id(n: int) -> str:
    return f"U{n:09d}"          # illustrative user IDs


def post_id(n: int) -> str:
    return f"p{n:010d}"         # illustrative post IDs


def part(user: str, partitions: int) -> int:
    # One partition per user, the same function in every script, so a user's flushes stay in order.
    return zlib.crc32(user.encode()) % partitions


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
