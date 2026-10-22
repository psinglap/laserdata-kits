"""Shared settings for the customer memory lane kit: every turn reaches the memory builder in milliseconds, and each
customer's updated memory reaches the store every channel's agent reads, before that customer's next message on
any channel, on Apache Iggy."""
import asyncio
import json
import random
import time
import zlib

STREAM = "agent-memory"
TURNS = "turns"                 # the lane: every turn of every conversation, keyed by customer
MEMORY = "customer-memory"      # the memory builder's output: each customer's updated memory, keyed by customer
TRANSCRIPTS = "transcripts"     # a heavy, latency-tolerant transcript log on the same broker (lakehouse, Insights)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {TURNS: 16, MEMORY: 4, TRANSCRIPTS: 32}

# Where conversations happen and how far apart a customer's turns are (illustrative; set them to yours).
CHANNELS = ("voice", "chat", "sms")
CHANNEL_SHARE = (0.55, 0.35, 0.10)
GAP_S = {"voice": (2.0, 8.0), "chat": (4.0, 30.0), "sms": (5.0, 60.0)}

# Facts a turn can add to a customer's memory (illustrative; your memory extraction decides what is kept).
FACTS = ("order 58213 arrived damaged", "prefers a text over a call", "rebooked to the 18:40 departure",
         "refund promised by Friday", "moving house on the 3rd", "upgrade quoted at the loyalty price",
         "lost card reported, replacement sent", "appointment moved to Tuesday 9:30")


def customer(i: int) -> str:
    return f"cust-{i:07d}"


def part(key: str, partitions: int) -> int:
    # One partition per customer, the same function in every script, so a customer's turns stay in order.
    return zlib.crc32(key.encode()) % partitions


def pick_channel(other_than: str | None = None) -> str:
    while True:
        ch = random.choices(CHANNELS, CHANNEL_SHARE)[0]
        if ch != other_than:
            return ch


def next_gap(channel: str) -> float:
    lo, hi = GAP_S[channel]
    return random.uniform(lo, hi)


def switch_gap(quick_share: float) -> float:
    """Seconds from the last turn on one channel to the first message on the next (illustrative). A share open a
    link the agent just texted, 1-5 s later; the rest come back later, 5-120 s."""
    if random.random() < quick_share:
        return random.uniform(1.0, 5.0)
    return random.uniform(5.0, 120.0)


def mean_gap() -> float:
    """Seconds between turns, averaged over turns: customers on voice take turns faster than on SMS."""
    return 1 / sum(s * 2 / (GAP_S[c][0] + GAP_S[c][1]) for c, s in zip(CHANNELS, CHANNEL_SHARE))


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
