"""Shared settings for the cross-shard order group lane: every fill on every matching engine shard reaches the
order group tracker in milliseconds, and every trigger reaches every shard, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "exchange"
FILLS = "order-group-fills"     # the lane: every fill in an order group, from every shard, keyed by order group
STATES = "order-group-states"   # the tracker's triggered and reset states, back to every shard
EXPORT = "trade-export"         # a heavy, latency-tolerant stream on the same broker: public trades for the data lake
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {FILLS: 16, STATES: 1, EXPORT: 8}

WINDOW_S = 15                   # Kalshi order groups count filled contracts over a rolling 15-second window

# Kalshi's published shard assignments (Exchange Sharding docs, Oct 2026). Shard 0 is the catch-all.
SHARDS = {0: "Default", 1: "Combos", 2: "Crypto", 3: "Sports"}
# Illustrative market tickers per shard, for readable output only.
MARKETS = {
    0: ["KXNFLGAME-26OCT25KCLV-KC", "KXFEDDECISION-26DEC-H0", "KXHIGHNY-26OCT22-B62.5"],
    1: ["KXMVENBASINGLEGAME-26OCT22LALGSW-R7", "KXMVENFLCOMBO-26OCT25-R3"],
    2: ["KXBTCD-26OCT2217-T108500", "KXETHD-26OCT2217-T3900"],
    3: ["KXNBAGAME-26OCT22LALGSW-LAL", "KXMLBGAME-26OCT22NYYLAD-NYY", "KXATPMATCH-26OCT22SINALC-SIN"],
}
AVG_CONTRACTS = 25              # mean contracts per fill (illustrative; fills are 1-49 contracts)


def group(i: int) -> str:
    return f"og-{i:06d}"


def part(key: str, partitions: int) -> int:
    # One partition per order group, the same function in every script, so a group's fills stay together.
    return zlib.crc32(key.encode()) % partitions


def shards_of(g: str) -> list[int]:
    """The shards a group's orders rest on (illustrative): a market maker quoting one game on its single-game
    markets and on the combos that include it spans two or three shards."""
    h = zlib.crc32(g.encode())
    home = 3 if h % 2 else 0                # basketball, baseball and tennis on 3; football and the rest on 0
    out = [home, 1]                         # its combos on shard 1
    if h % 5 == 0:
        out.append(2)                       # some makers also quote crypto in the same group budget
    return out


def weight(g: str) -> float:
    # Some groups trade far more than others; a few are heavy.
    return 0.3 + 2.7 * ((zlib.crc32((g + "/w").encode()) % 1000) / 1000) ** 2


def limit(g: str, fills_per_s: float, groups: int) -> int:
    """A group's contracts limit over the rolling window (illustrative): between 1x and 3x what the group fills
    in a window on average, so a busy stretch on one game takes it over the limit and the rest stay under."""
    even = fills_per_s * AVG_CONTRACTS * WINDOW_S * weight(g) / (groups * 1.2)
    f = (zlib.crc32((g + "/l").encode()) % 1000) / 1000
    return max(10, int(even * (1.0 + 2.0 * f)))


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
