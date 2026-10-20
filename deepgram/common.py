"""Shared settings for the streaming usage lane kit: every usage message from the speech engine reaches the
per-project usage summary in milliseconds, counted once even when it is sent twice, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "metering"
USAGE = "usage"                    # the lane: each live stream's billable audio so far, keyed by project
SUMMARIES = "usage-summaries"      # per-project, per-minute usage, the resulting totals, for the Console and billing sync
LOGS = "request-logs"              # a heavy, latency-tolerant stream on the same broker: per-request logs
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {USAGE: 16, SUMMARIES: 1, LOGS: 32}

# Streaming speech-to-text list prices, USD per audio minute, pay-as-you-go (deepgram.com/pricing, read Oct 2026,
# shown there as limited-time rates). The kit only uses them to put a dollar figure on audio seconds; set yours.
MODELS = {"nova-3": 0.0048, "nova-3-multi": 0.0058, "flux": 0.0065}
MODEL_MIX = ["nova-3"] * 6 + ["flux"] * 3 + ["nova-3-multi"]       # illustrative traffic mix


def project(i: int) -> str:
    return f"proj_{i:05d}"


def accessor(proj: str, i: int) -> str:
    return f"key_{zlib.crc32(proj.encode()) % 100_000:05d}{i}"     # one of a project's API keys


def part(key: str, partitions: int) -> int:
    # One partition per project, the same function in every script, so a project's usage stays in order.
    return zlib.crc32(key.encode()) % partitions


def usd(audio_ms: float, model: str) -> float:
    return audio_ms / 60_000 * MODELS.get(model, MODELS["nova-3"])


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
