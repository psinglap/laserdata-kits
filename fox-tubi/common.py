"""Shared settings for Tubi's creative readiness lane: every programmatic creative an ad server meets for the first
time reaches the creative registry in milliseconds, is sent to transcoding and brand recognition once, and its
verdict (ready with brand and category, or held) reaches every ad server node in milliseconds, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "tubi-ads"
SEEN = "creative-seen"          # the lane in: an ad server node met a creative it has no verdict for, keyed by creative
READY = "creative-ready"        # the lane out: the registry's verdicts, read by every ad server node
AD_EVENTS = "ad-events"         # a heavy, latency-tolerant impression and quartile beacon log on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {SEEN: 16, READY: 1, AD_EVENTS: 32}

# Ad categories used for competitive separation in a pod (no two ads of one category in the same break).
# Names are illustrative; your ad server's category list replaces them.
CATEGORIES = ["auto", "telecom", "quick-service", "insurance", "pharma", "retail", "streaming", "beverage",
              "household", "finance", "travel", "gaming", "beauty", "home", "entertainment", "government"]
DSPS = [f"dsp-{i:02d}" for i in range(1, 13)]     # programmatic partners, anonymized
BRANDS = 600                                      # canonical parent brands in the knowledge graph (illustrative)


def creative_id(n: int) -> str:
    return f"cr-{n:07d}"


def h(key: str, salt: str = "") -> int:
    return zlib.crc32((salt + key).encode())


def part(key: str, partitions: int) -> int:
    # One partition per creative, the same function in every script, so a creative's sightings stay in order
    # and land on one registry reader, which starts its job once.
    return h(key) % partitions


def attributes(cid: str) -> dict:
    """What transcoding and brand recognition would conclude about a creative. Deterministic stand-in: your
    pipeline's result goes here. About 3% are held (failed standards review or transcode)."""
    brand = h(cid, "brand") % BRANDS
    return {"brand": f"brand-{brand:04d}", "cat": CATEGORIES[h(f"{brand}", "cat") % len(CATEGORIES)],
            "status": "held" if h(cid, "qc") % 100 < 3 else "ready"}


def base_cpm(cid: str) -> float:
    """A creative's typical bid in USD CPM, $10-$35 (illustrative; your bid data replaces it)."""
    return 10 + 25 * (h(cid, "cpm") % 1000) / 1000


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
