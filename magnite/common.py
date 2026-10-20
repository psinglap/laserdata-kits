"""Shared settings for the DSP appetite lane kit: every change in which DSPs are bidding on which supply
reaches every traffic-shaping node in milliseconds, on Apache Iggy.

A message on the lane is the exchange's latest view of one (DSP, supply segment) pair: its recent bid
rate and whether the DSP is bidding on that segment at all. It carries the resulting value, never a
delta, with a per-pair sequence number and the time the exchange saw the change, so any shaping node
can start from any message.
"""
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "demand"
APPETITE = "dsp-appetite"            # the lane: one message per change in a (DSP, segment) pair, keyed by the pair
AUCTION_LOG = "auction-log"          # a heavy, latency-tolerant auction log on the same broker (the analytics feed)
PARTITIONS = {APPETITE: 8, AUCTION_LOG: 16}
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=6))   # enough for a new shaping pod to rebuild, and for replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: no delayed-ACK stalls on small writes


def pair(dsp: int, seg: int) -> str:
    # A supply segment stands for whatever your shaping model keys on: publisher, format (CTV, video, display,
    # audio), device, geo. The kit only needs a stable ID.
    return f"dsp-{dsp:03d}|seg-{seg:05d}"


def pairs(dsps: int, segments: int) -> list:
    return [pair(d, s) for d in range(dsps) for s in range(segments)]


def part(key: str) -> int:
    # The same function in every script, so a pair's changes always land on one partition, in order.
    return zlib.crc32(key.encode()) % PARTITIONS[APPETITE]


def msg(k: str, seq: int, rate: float, on: bool, flip: bool = False, waited_ns: int = 0) -> bytes:
    # Created time = now, minus how long the change has already waited for its write.
    return json.dumps({"k": k, "q": seq, "r": round(rate, 4), "on": int(on), "f": int(flip),
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
