"""Shared settings for the spend-to-pacing lane kit: every win's spend reaches the pacing controllers in
milliseconds, and their pause and throttle decisions reach every bid server, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "bidding"
SPEND = "spend"                 # the lane: each bid server's running spend per ad group, keyed by ad group
PACING = "pacing-decisions"     # the controllers' decisions back to every bid server: open, throttle or pause
FEEDBACK = "bid-feedback"       # a heavy, latency-tolerant bid-feedback log on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090"
PARTITIONS = {SPEND: 16, PACING: 1, FEEDBACK: 32}


def ad_group(i: int) -> str:
    return f"ag-{i:05d}"


def part(key: str, partitions: int) -> int:
    # One partition per ad group, the same function in every script, so an ad group's spend stays in order.
    return zlib.crc32(key.encode()) % partitions


def flight(flight_s: float, now: float | None = None) -> int:
    # The kit compresses a campaign flight into --flight-s seconds; every script derives it from the clock.
    return int((time.time() if now is None else now) // flight_s)


def budget(ag: str, wins_per_s: float, ad_groups: int, flight_s: float) -> float:
    """Each ad group's budget for one flight, in USD (illustrative). About a third are always-on with a large
    budget, so traffic never stops; the rest get 30-100% of an even share and reach their budget mid-flight."""
    even = wins_per_s * AVG_PRICE * flight_s / ad_groups
    h = zlib.crc32(ag.encode())
    return round(even * (6.0 if h % 3 == 0 else 0.3 + 0.7 * (h % 1000) / 1000), 4)


# Clearing prices: $1.50-$12.00 CPM, so $0.0015-$0.012 per impression (illustrative; set them to yours).
CPM_LOW, CPM_HIGH = 1.5, 12.0
AVG_PRICE = (CPM_LOW + CPM_HIGH) / 2 / 1000


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
