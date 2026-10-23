"""Shared settings for the match termination lane kit: every combat action a VALORANT game server records reaches
the anti-cheat scorer in milliseconds, and every match termination reaches the game server running that match
before the cheater's next shot lands, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "gameplay-integrity"
ACTIONS = "combat-actions"         # the lane: one record per shot a game server records, keyed by match
TERMINATIONS = "match-terminations"   # the anti-cheat scorer's terminations, read by every game server host
ANALYTICS = "analytics-telemetry"  # a heavy, latency-tolerant stream on the same broker: analytics events
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {ACTIONS: 16, TERMINATIONS: 1, ANALYTICS: 32}
WEAPONS = ("vandal", "phantom", "sheriff", "operator", "spectre", "ghost")


def match_id(shard: str, host: int, n: int) -> str:
    # Illustrative IDs: shard, game server host and a per-host match counter.
    return f"{shard}-g{host:02d}-m{n:06d}"


def player_id(i: int) -> str:
    return f"p{zlib.crc32(str(i).encode()):08x}"


def part(match: str, partitions: int) -> int:
    # One partition per match, the same function in every script, so a match's shots stay in order.
    return zlib.crc32(match.encode()) % partitions


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
