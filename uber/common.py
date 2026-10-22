"""Shared settings for the surge window lane kit: every demand and supply event, keyed by hexagon, reaches the
surge window that owns it in milliseconds, inside the time window it belongs to, and the surge workers in two
regions read the same log and close every window with the same counts, on Apache Iggy."""
import asyncio
import hashlib
import json
import time
import zlib

STREAM = "marketplace"
EVENTS = "surge-events"            # the lane: demand (rider) and supply (driver) events, keyed by hexagon
MULTIPLIERS = "surge-multipliers"  # the primary region's result: one multiplier per hexagon and product per window
DIGESTS = "window-digests"         # each region's fingerprint of each closed window, to check the regions agree
TRUTH = "window-truth"             # the marketplace's own distinct counts per window (the independent check)
ANALYTICS = "app-analytics"        # a heavy, latency-tolerant stream on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {EVENTS: 16, MULTIPLIERS: 4, DIGESTS: 1, TRUTH: 1, ANALYTICS: 16}

PRODUCTS = ["UberX", "Comfort", "UberXL"]
CITIES = ["sf", "nyc", "chi", "la", "sea", "bos", "atl", "mia"]


def hexagon(city: str, i: int) -> str:
    # A stand-in for an H3 cell ID: the kit needs one stable key per hexagon, not real geometry.
    return f"{city}-{i:05d}"


def part(hex_id: str, partitions: int) -> int:
    # One partition per hexagon, the same function in every script, so all of a hexagon's events are in order
    # on one partition and a surge worker sees the whole hexagon.
    return zlib.crc32(hex_id.encode()) % partitions


def window(t_ms: int, window_s: float) -> int:
    return int(t_ms // int(window_s * 1000))


def multipliers(rows: dict) -> dict:
    """A placeholder for the surge model (Uber's runs a machine-learning algorithm in Flink). rows maps
    "hexagon|product" to [distinct riders, distinct available drivers] for one window. A cell whose
    riders-per-driver ratio is well above its city's ratio for that product gets a multiplier from 1.1x to 3.0x;
    every other cell stays at 1.0x. Deterministic, so two regions with the same counts get the same result."""
    city = {}
    for k, (d, s) in rows.items():
        h, p = k.split("|")
        c = city.setdefault((h.split("-")[0], p), [0, 0])
        c[0] += d
        c[1] += s
    out = {}
    for k, (d, s) in rows.items():
        h, p = k.split("|")
        cd, cs = city[(h.split("-")[0], p)]
        base = cd / max(cs, 1)
        rel = (d / max(s, 1)) / base if base and d >= 3 else 0.0
        out[k] = round(min(3.0, 1.0 + 0.25 * (rel - 2.0)), 1) if rel >= 2.4 else 1.0
    return out


def digest(rows: dict, mult: dict) -> str:
    """A fingerprint of one closed window: every (hexagon, product) -> distinct riders, drivers, multiplier."""
    body = "\n".join(f"{k}:{v[0]}:{v[1]}:{mult[k]}" for k, v in sorted(rows.items()))
    return hashlib.sha1(body.encode()).hexdigest()[:16]


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
