"""Shared settings for the real-time tracking lane kit: LinkedIn's latency-sensitive tracking signals on Apache Iggy.

LinkedIn's rebuilt Tracking API (LinkedIn Engineering, Oct 2026) "sends latency-sensitive signals directly to
real-time processing, while Apache Kafka provides the asynchronous boundary for near-real-time and offline
processing". This kit gives that direct path a log of its own: Tracking API writes the latency-sensitive signals
to one topic, keyed by a pseudonymized member ID with a per-member sequence number, and real-time processing
reads it partition by partition in milliseconds. A reader that restarts finds what was sent while it was away
waiting in the log. Bursty media events go on their own topic, as LinkedIn isolates its tracking categories.
"""
import asyncio
import json
import time
import zlib

STREAM = "tracking"
RT = "rt-signals"          # latency-sensitive tracking signals, keyed by pseudonymized member ID
MEDIA = "media-playback"   # bursty media events (render, play, pause, exit) on their own topic
PARTITIONS = {RT: 16, MEDIA: 16}
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

# Illustrative event names; map yours in kafka_bridge.py.
SIGNALS = [("FeedImpressionEvent", 0.55), ("PageViewEvent", 0.20), ("FeedActionEvent", 0.12),
           ("SearchActionEvent", 0.08), ("MessageSendEvent", 0.05)]
PLATFORMS = [("android", 0.45), ("ios", 0.35), ("web", 0.20)]


def member(n: int) -> str:
    # a stable pseudonymized ID per member, as LinkedIn uses for tracing decisions; never the member ID itself
    return f"pm-{zlib.crc32(n.to_bytes(8, 'little')):08x}{n % 251:02x}"


def part(key: str, partitions: int) -> int:
    # One partition per member, the same function in every script, so a member's signals stay in order.
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
