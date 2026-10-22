"""Shared settings for the Dailyhunt floor lane kit: every new ad floor for a Dailyhunt placement reaches
every NexVerse.ai bid node in milliseconds, on Apache Iggy.

A message on the lane is the latest floor for one placement (language x ad slot): the floor in rupees CPM
and whether this change is a step (a jump up or down, as when breaking news moves demand on one language)
or a small move. It carries the resulting floor, never a delta, with a per-placement sequence number and
the time the floor service set it, so any bid node can start from any message.
"""
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "nexverse"
FLOORS = "floors"                    # the lane: one message per floor change, keyed by placement
AD_EVENTS = "ad-events"              # a heavy, latency-tolerant ad event log on the same broker (the analytics feed)
PARTITIONS = {FLOORS: 8, AD_EVENTS: 16}
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=6))   # enough for a new bid node to rebuild, and for replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: no delayed-ACK stalls on small writes

# Dailyhunt serves 15 Indian languages (VerSe); the codes only need to be stable IDs.
LANGS = ["hi", "bn", "mr", "te", "ta", "kn", "ml", "gu", "or", "pa", "ur", "as", "bho", "ne", "en"]
SURFACES = ["feed", "article", "video", "buzz"]


def placement(lang: str, n: int) -> str:
    # A placement stands for whatever your floors are keyed on: language, surface, slot position, state,
    # device. The kit only needs a stable ID, and the language first so a spike can pick one language.
    return f"{lang}|{SURFACES[n % len(SURFACES)]}|{n:04d}"


def placements(per_lang: int, langs: int = len(LANGS)) -> list:
    return [placement(l, n) for l in LANGS[:langs] for n in range(per_lang)]


def part(key: str) -> int:
    # The same function in every script, so a placement's floors always land on one partition, in order.
    return zlib.crc32(key.encode()) % PARTITIONS[FLOORS]


def msg(k: str, seq: int, floor: float, step: int = 0, waited_ns: int = 0) -> bytes:
    # step: +1 a jump up, -1 a jump down, 0 a small move or a repeat. Created time = now, minus how long
    # the change has already waited for its write.
    return json.dumps({"k": k, "q": seq, "f": round(floor, 2), "s": step,
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
