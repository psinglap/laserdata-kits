"""Shared settings for the publish-to-site lane: every published version of every asset, keyed by asset URI,
to every serving node, on Apache Iggy.

A message on the lane is one version of one top-level asset as the Publishing Pipeline's denormalized log
carries it: the asset's URI (nyt://article/...), its version (one more on every publish of that asset), why it
was written (an editor publish, an update, a correction, or a republish because something it references
changed), the headline and a body of about the size of a bundled article, plus the time it was created. The
version is the whole asset, never a diff, so a serving node can serve from the newest message alone.
"""
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "nyt-publishing"
LANE = "published"                   # one copy of every published asset version, keyed by top-level asset URI
PARTITIONS = 8                       # an asset always lands on one partition, so its versions stay in order
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))   # a news day; the Monolog in Kafka keeps everything since 1851
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
SLA_MS = 1000                        # "one second after the editor clicks publish, the latest version has to be available"

# Why a version was written. Editor actions are what the one-second SLA is about; republishes come from the
# denormalizer when a referenced image, byline or tag changes, and can arrive by the thousand.
EDITOR = ("publish", "update", "correction", "post")
REPUBLISH = "republish"

# The live desk: what is being updated right now and how often (seconds between versions, real time).
KINDS = {"live": 45, "breaking": 120, "article": 900}
MIX = ["live"] * 2 + ["breaking"] * 3 + ["article"] * 15


def partition_of(uri: str) -> int:
    # The same function everywhere, so an asset's versions always land on one partition, in order.
    return zlib.crc32(uri.encode()) % PARTITIONS


def live_uris(n: int) -> list:
    # Assets on the live desk today. Illustrative ids in the shape of the pipeline's nyt:// URIs.
    return [f"nyt://article/{zlib.crc32(f'live-{i}'.encode()):08x}-{i:04d}" for i in range(n)]


def archive_uris(n: int) -> list:
    # Older articles that reference one tag; all of them are republished when that tag changes.
    return [f"nyt://article/{zlib.crc32(f'arch-{i}'.encode()):08x}-a{i:05d}" for i in range(n)]


def kind_of(i: int) -> str:
    return MIX[i % len(MIX)]


def readership(i: int) -> float:
    # A handful of live and breaking stories draw most readers; a long tail of articles draws the rest.
    return 1.0 / (i + 1) ** 1.2


BODY = ("The quick reporting desk files another paragraph. " * 400)


def pack(uri: str, version: int, why: str, headline: str, body_bytes: int, waited_ns: int = 0) -> bytes:
    # Created time = now, minus how long the version has already waited for its write.
    return json.dumps({"u": uri, "v": version, "w": why, "h": headline, "b": BODY[:body_bytes],
                       "ns": time.time_ns() - waited_ns}, separators=(",", ":")).encode()


def unpack(b: bytes) -> dict:
    return json.loads(b)


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topic(c: IggyClient, topic: str, partitions: int):
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    if await c.get_topic(STREAM, topic) is None:
        try:
            await c.create_topic(STREAM, topic, partitions, message_expiry=RETENTION)
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
