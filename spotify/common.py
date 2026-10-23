"""Shared settings for the episode publish status lane kit, on Apache Iggy.

Every upload's receipt and every stage after it (received and queued, transcoding, transcoded, content analysis
done, live), keyed by episode, reaches the status service in milliseconds. The status service answers the
creator ("received and queued", or "already received" for a re-upload) and watches how long the oldest new
episode has waited in each priority queue, so a transcoding backlog pages within seconds of crossing its line.

A message on the lane is one stage change of one episode: the episode key (show and content digest), the stage
it is now in (the resulting state, not a delta), its priority queue (new episodes or updates to older ones),
the time it was queued, a per-episode event number n, and the time it was published. n is the event ID: a
repeat (an at-least-once redelivery, a replay) carries the same n and changes nothing.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "podcast-publishing"
STATUS = "episode-status"          # the lane: every stage change, keyed by episode
REPROCESS = "reprocess-log"        # a heavy, latency-tolerant neighbour: the reprocessing batch job's segment log
PARTITIONS = {STATUS: 16, REPROCESS: 16}
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=48))   # two days of stage changes, for rebuilds and review
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

# Stages in order; the stage number is how far the episode has come.
STAGES = ("received", "transcoding", "transcoded", "analyzed", "live")
NEW, UPDATE = "new", "update"      # the medium-priority queue (new episodes) and the low-priority one (updates)


def episode(show: int, upload: int) -> str:
    # An illustrative key: the show and a digest of the uploaded file, so a re-upload of the same file maps to
    # the same episode. Yours would come from the upload API (show ID plus the file's content hash).
    return f"show-{show:06d}:{zlib.crc32(f'{show}/{upload}'.encode()):08x}"


def part(key: str, partitions: int) -> int:
    # The same function in every script, so an episode's stage changes always land on one partition, in order.
    return zlib.crc32(key.encode()) % partitions


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
                await c.create_topic(STREAM, t, PARTITIONS[t], message_expiry=RETENTION)
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
