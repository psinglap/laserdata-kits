"""Shared settings for the Mux Data streaming export lane kit: one Apache Iggy topic per streaming export.

Mux Data streams finished video views to each Media-plan customer's own Amazon Kinesis data stream
or Google Cloud Pub/Sub topic, "within about one minute after the view ends", and today runs those
exports on Kafka Connect. Here every streaming export (one environment, one destination) gets its
own topic in the view-exports stream, keyed by view ID. The finished-view writer (views.py) appends
each finished or updated view to its environment's export topic; each export worker (exporter.py)
reads its exports at their destination's pace from their own offsets, so a customer's stream that
is throttled or unreachable only ever delays its own export, and the log keeps the views it has not
taken yet.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "view-exports"
BEACON_STREAM = "beacons"                                  # raw player beacons next door (beacon_flood.py)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PROMISE_S = 60                                             # "within about one minute after the view ends"
HOLD = timedelta(hours=24)                                 # how long an export's views wait for a destination that is down
RETENTION = IggyExpiry.ExpireDuration(HOLD)
KINESIS_SHARD_LIMIT = 1000                                 # records a second one Kinesis shard accepts (AWS quota)

# Fields of a finished view, named after the VideoView protobuf in muxinc/mux-protobuf (a subset).
PLAYERS = ("mux-player", "video.js", "hls.js", "exoplayer", "avplayer", "shaka-player")
COUNTRIES = ("US", "GB", "IN", "BR", "DE", "JP", "MX", "CA", "AU", "FR")


def env_name(i: int) -> str:
    return f"env-{i + 1:04d}"


def destination(i: int) -> str:
    """Odd environments export to Kinesis, even ones to Pub/Sub (illustrative split)."""
    return "kinesis" if i % 2 == 0 else "pubsub"


def export_name(i: int) -> str:
    return f"{env_name(i)}-{destination(i)}"


def weights(n: int) -> list:
    """Views per environment: a few very large media customers and a long tail (Zipf-like), summing to 1."""
    w = [1 / (i + 1) ** 0.9 for i in range(n)]
    s = sum(w)
    return [x / s for x in w]


def partitions_for(i: int, n: int) -> int:
    # The largest 5% of environments get 4 partitions (and a 4-shard Kinesis stream); everyone else 1.
    return 4 if i < max(1, n // 20) else 1


def exports(n: int) -> dict:
    """Every streaming export of the kit's n environments: topic name -> (env index, destination, partitions)."""
    return {export_name(i): (i, destination(i), partitions_for(i, n)) for i in range(n)}


def view_id(env: int, k: int) -> str:
    return f"{env:04x}{k:012x}-v"                            # illustrative view IDs, unique per run


def shard_of(topic: str, shards: int) -> int:
    return zlib.crc32(topic.encode()) % shards


def part(view: str, partitions: int) -> int:
    # Keyed by view ID: a view and its later updates stay in order on one partition.
    return zlib.crc32(view.encode()) % partitions


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
