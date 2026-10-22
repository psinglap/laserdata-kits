"""Shared settings for the KBC Play Along answer lane: every locked answer reaches the scorer that owns its
player, on time against the question's close and counted once, before the answer is revealed on TV, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "kbc-play-along"
LOCKS = "answer-locks"           # the lane: every locked answer, keyed by player
SHOW = "show-desk"               # the question timeline from the show desk: open, close, reveal (one partition, in order)
ENGAGE = "live-engagement"       # a heavy, latency-tolerant stream on the same broker (playback and engagement events)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {LOCKS: 16, SHOW: 1, ENGAGE: 32}

# KBC Play Along points per question level (KBC 2025 Play Along FAQ, question 7): Rs 5,000 on air is 50 points,
# Rs 7 crore is 7,00,000 points. A lock inside the first 10 seconds earns 200 more.
POINTS = [50, 100, 150, 200, 250, 500, 1_000, 2_000, 3_000, 5_000, 7_500, 12_500, 25_000, 50_000, 1_00_000, 7_00_000]
FAST_BONUS, FAST_S = 200, 10


def player_id(n: int) -> str:
    return f"PAP{n:09d}"         # illustrative Play Along player IDs (the app identifies a player by phone number)


def question_id(episode: int, n: int) -> str:
    return f"E{episode:03d}-Q{n:02d}"


def part(player: str, partitions: int) -> int:
    # One partition per player, the same function in every script, so a player's locks stay in order.
    return zlib.crc32(player.encode()) % partitions


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
