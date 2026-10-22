"""Shared settings for the REV2 safety signal lane kit.

Three kinds of event, all keyed by content ID (t3_ for posts, t1_ for comments) with the same partition
function, so a post or comment and every ML signal about it land on the same partition number of their
topics:
  * content-preliminary: the preliminary enriched content (who posted, where, the text) that REV2 reads;
  * signal-<name>: one topic per ML signal, written by the ML service that scores the content;
  * content-enriched: the fully enriched content, written by the signals joiner as soon as the last
    expected signal for that content has arrived (or, past the wait limit, without it).
A signals joiner owns a fixed set of partition numbers and reads those partitions of every input topic
directly, so it sees a piece of content and all of its signals, with no shuffle and no rebalance.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "safety"
PRELIM_TOPIC = "content-preliminary"
ENRICHED_TOPIC = "content-enriched"
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))          # a day kept, for Time-Travel and replay

# ML signals (illustrative names; yours are your own). Every post and comment gets a text and a spam score;
# posts with an image or video also get a media score. Inference delay is lognormal around the median, in ms.
SIGNALS = {
    "text_harm": {"median_ms": 250, "applies": "all"},
    "spam": {"median_ms": 120, "applies": "all"},
    "media_safety": {"median_ms": 900, "applies": "media"},
}
ACTION_AT = 0.97                                   # the toy REV2 rule acts when any score is at or above this


def signal_topic(name: str) -> str:
    return f"signal-{name.replace('_', '-')}"


def expected(kind: str, media: int) -> list:
    return [s for s, d in SIGNALS.items() if d["applies"] == "all" or (media and d["applies"] == "media")]


def content_id(n: int, kind: str) -> str:
    # Reddit-style fullname: t3_ for a post, t1_ for a comment, then a base-36 ID (illustrative).
    digits, x = "0123456789abcdefghijklmnopqrstuvwxyz", n + 36 ** 6
    s = ""
    while x:
        x, r = divmod(x, 36)
        s = digits[r] + s
    return ("t3_" if kind == "post" else "t1_") + s


def part(cid: str, partitions: int) -> int:
    # The same function everywhere: a piece of content and all its signals share one partition number.
    return zlib.crc32(cid.encode()) % partitions


def owner(pid: int, joiners: int) -> int:
    # Static ownership: partition number p (of every input topic) belongs to joiner p % joiners.
    return pid % joiners


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


async def ensure_all(c: IggyClient, partitions: int) -> int:
    """Create every lane topic with the same partition count; return the count actually in place."""
    for t in [PRELIM_TOPIC, ENRICHED_TOPIC] + [signal_topic(s) for s in SIGNALS]:
        await ensure_topic(c, STREAM, t, partitions)
    counts = {(await c.get_topic(STREAM, t)).partitions_count
              for t in [PRELIM_TOPIC, ENRICHED_TOPIC] + [signal_topic(s) for s in SIGNALS]}
    if len(counts) != 1:
        raise SystemExit(f"lane topics have different partition counts {sorted(counts)}: recreate them")
    return counts.pop()


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
