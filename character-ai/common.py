"""Shared settings for the safety verdict lane kit: every chat turn reaches the trust and safety classifiers
in milliseconds, and their verdicts reach every chat server before that conversation's next turn, on Apache Iggy."""
import asyncio
import json
import random
import time
import zlib

STREAM = "chat-safety"
TURNS = "turns"                 # the lane: each turn's user message and the character's reply, keyed by conversation
VERDICTS = "safety-verdicts"    # the classifiers' verdicts back to every chat server: block or lifeline
PROMPTS = "full-prompts"        # a heavy, latency-tolerant full-prompt log on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {TURNS: 16, VERDICTS: 1, PROMPTS: 32}

# What a classifier can return for a turn (illustrative; your policy names go here).
BLOCK = "block"                 # the content is blocked from the conversation, so the next prompt must not carry it
LIFELINE = "lifeline"           # self-harm reference: the next reply shows the Lifeline pop-up


def conversation(i: int) -> str:
    return f"conv-{i:07d}"


def part(key: str, partitions: int) -> int:
    # One partition per conversation, the same function in every script, so a conversation's turns stay in order.
    return zlib.crc32(key.encode()) % partitions


def next_gap(swipe_share: float) -> float:
    """Seconds until a conversation's next turn (illustrative; set it to yours). Most turns are typed replies,
    3-30 s apart; a share are swipes or regenerations of the last reply, 0.3-3 s apart."""
    if random.random() < swipe_share:
        return random.uniform(0.3, 3.0)
    return random.uniform(3.0, 30.0)


def mean_gap(swipe_share: float) -> float:
    return swipe_share * 1.65 + (1 - swipe_share) * 16.5


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
