"""Shared settings for the device throttle lane kit: every ingestion node's accepted event counts reach the
throttle service in milliseconds, and every throttle reaches every ingestion node, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "ingestion"
COUNTS = "device-counts"        # the lane: each ingestion node's accepted events per device per second, keyed by device
STATE = "throttle-state"        # the throttle service's decisions back to every ingestion node: throttled or ok
REPLAY = "session-replay"       # a heavy, latency-tolerant stream on the same broker: Session Replay segments
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {COUNTS: 16, STATE: 1, REPLAY: 32}

# From Amplitude's HTTP V2 API docs: devices and users that exceed 30 events per second, "measured as an
# average over a recent time window", are throttled with a 429; the client pauses that device for 30 s.
# The Batch API documents 1,000 events per second averaged over 30 s. Set --threshold to the endpoint you test.
THRESHOLD_EPS = 30
WINDOW_S = 30
BACKOFF_S = 30


def device(i: int) -> str:
    return f"dev-{i:07d}"


def part(key: str, partitions: int) -> int:
    # One partition per device, the same function in every script, so a device's counts stay in order.
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


class Rate:
    """Events per second for one device, averaged over the last `window` whole seconds (1-second buckets)."""
    __slots__ = ("b",)

    def __init__(self):
        self.b = {}

    def add(self, sec: int, n: int):
        self.b[sec] = self.b.get(sec, 0) + n

    def avg(self, now_sec: int, window: int = WINDOW_S) -> float:
        lo = now_sec - window + 1
        for s in [s for s in self.b if s < lo]:
            del self.b[s]
        return sum(self.b.values()) / window
