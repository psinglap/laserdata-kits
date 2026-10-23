"""Shared settings for the Roku Exchange live floor lane: every auction outcome reaches the floor service in
milliseconds, and every floor it recomputes reaches every exchange node, on Apache Iggy."""
import asyncio
import json
import math
import random
import time
import zlib
from itertools import product

STREAM = "roku-exchange"
OUTCOMES = "auction-outcomes"   # the lane: one record per auction, keyed by inventory segment
FLOORS = "floor-updates"        # the floor service's floors back to every exchange node
IMPRESSIONS = "impression-log"  # a heavy, latency-tolerant impression and bid log on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {OUTCOMES: 16, FLOORS: 1, IMPRESSIONS: 32}

# An inventory segment is content category x device type x position in the ad break. Content category, not
# show: show-level identifiers stay out of the bid stream. Names are illustrative; set them to your segments.
CATEGORIES = ["sports", "news", "movies", "drama", "comedy", "reality", "kids", "documentary", "food", "home",
              "music", "crime", "sci-fi", "anime", "classic-tv", "westerns", "game-shows", "faith", "latino",
              "local-news"]
DEVICES = ["roku-tv", "stick", "player", "other"]
BREAK_SLOTS = ["pre", "mid1", "mid2", "mid3", "mid4"]
LIVE = "sports"                 # the category whose live ad breaks bring a surge of demand


def segments(n: int) -> list[str]:
    return ["/".join(s) for s in product(CATEGORIES, DEVICES, BREAK_SLOTS)][:n]


def part(key: str, partitions: int) -> int:
    # One partition per segment, the same function in every script, so a segment's outcomes stay in order.
    return zlib.crc32(key.encode()) % partitions


def base_cpm(seg: str) -> float:
    """A segment's typical bid in USD CPM, $8-$30 by content category (illustrative; your bid data replaces it)."""
    return 8 + 22 * (zlib.crc32(seg.split("/")[0].encode()) % 1000) / 1000


def demand(seg: str, t: float, a) -> float:
    """Demand density multiplier at second t: a slow +/-15% drift per segment, and during a live game's ad
    break (--surge-for seconds out of every --surge-every) the live category gets --surge times the bidders."""
    m = 1 + 0.15 * math.sin(2 * math.pi * (t / 120 + (zlib.crc32(seg.encode()) % 100) / 100))
    if seg.startswith(LIVE + "/") and t % a.surge_every < a.surge_for:
        m *= a.surge
    return m


def poisson(lam: float, rng=random) -> int:
    k, p, limit = 0, 1.0, math.exp(-lam)
    while True:
        p *= rng.random()
        if p < limit:
            return k
        k += 1


def bids(seg: str, t: float, a, rng=random) -> list[float]:
    """The bids that came back in time for one auction, highest first, in USD CPM. More demand means more
    bidders and higher bids; a few bidders time out and are left out."""
    m = demand(seg, t, a)
    v = base_cpm(seg) * math.sqrt(m)
    out = [v * math.exp(rng.gauss(0, 0.35)) for _ in range(poisson(a.density * m, rng)) if rng.random() > 0.05]
    return sorted(out, reverse=True)


def clear(top: float, second: float, floor: float) -> float:
    """Second-price auction with a floor: no fill under the floor, otherwise the higher of second bid and floor."""
    if top <= 0 or top < floor:
        return 0.0
    return max(second, floor)


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
