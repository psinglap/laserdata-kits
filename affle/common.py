"""Shared settings for the click-to-install lane kit: every click and every install postback, keyed by device,
reaches the mFaaS click-to-install scorer in milliseconds, and every publisher block it makes reaches every
attribution node before that publisher's next conversion is billed, on Apache Iggy."""
import asyncio
import json
import random
import time
import zlib

STREAM = "affle-mfaas"
EVENTS = "click-install"        # the lane: clicks (with redirect time) and install postbacks, keyed by device
BLOCKS = "publisher-blocks"     # the scorer's verdicts back to every attribution node: one record per blocked publisher
BIDLOGS = "bid-logs"            # a heavy, latency-tolerant bid and win log for reporting, on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {EVENTS: 16, BLOCKS: 1, BIDLOGS: 32}

BLOCKED = "blocked"
CPCU_INR = 58.5                 # average CPCU, 9M FY2026 (Affle corporate presentation, Dec 2025); per conversion
INJECT_S = 10.0                 # click-to-install time under which an install looks injected (illustrative; yours)


def device(i: int) -> str:
    return f"aid-{i:08d}"       # stands in for the device ID (AffleID, GAID or IDFA) your trackers key on


def publisher(i: int) -> str:
    return f"pub-{i:04d}"       # a publisher or sub-publisher (site ID) on your supply


def part(d: str, partitions: int) -> int:
    # One partition per device, the same function in every script, so a device's click is always ahead of its
    # install on the lane.
    return zlib.crc32(d.encode()) % partitions


def honest_ctit(lo: float, hi: float) -> float:
    """Seconds from a genuine click to its install (illustrative and compressed so a one-minute run shows it;
    real click-to-install times run from minutes to days)."""
    return random.uniform(lo, hi)


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
