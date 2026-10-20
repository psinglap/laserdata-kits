"""Shared settings for the provider health lane kit: every turn's speech-to-text, LLM and voice outcome reaches
the health evaluator in milliseconds, and its degraded and recovered verdicts reach every call worker before
their calls' next turns, on Apache Iggy."""
import asyncio
import json
import random
import time
import zlib

STREAM = "voice-calls"
OUTCOMES = "provider-outcomes"   # the lane: one record per provider call in a turn, keyed by stage:provider:region
HEALTH = "provider-health"       # the evaluator's verdicts back to every call worker: degraded or ok
LOGS = "call-logs"               # a heavy, latency-tolerant call log (turn logs, end-of-call reports) on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {OUTCOMES: 16, HEALTH: 1, LOGS: 32}

# The three provider calls in a voice turn, two providers each, three regions (illustrative; your provider
# names, fallback order and regions go here).
STAGES = ("stt", "llm", "tts")
PROVIDERS = {"stt": ("stt-a", "stt-b"), "llm": ("llm-a", "llm-b"), "tts": ("tts-a", "tts-b")}
REGIONS = ("us", "eu", "au")
TTFB_MS = {"stt": (80, 220), "llm": (250, 700), "tts": (90, 250)}   # time to first byte when healthy (illustrative)

DEGRADED = "degraded"           # new turns start on the fallback; a small share still probes the primary
OK = "ok"                       # back to the primary


def key(stage: str, provider: str, region: str) -> str:
    return f"{stage}:{provider}:{region}"


def call_id(i: int) -> str:
    return f"call-{i:07d}"


def part(k: str, partitions: int) -> int:
    # One partition per stage:provider:region, the same function in every script, so a key's outcomes stay in order.
    return zlib.crc32(k.encode()) % partitions


def next_gap() -> float:
    """Seconds until a call's next turn (illustrative; set it to yours): a caller speaks, the agent answers."""
    return random.uniform(3.0, 9.0)


MEAN_GAP = 6.0


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
