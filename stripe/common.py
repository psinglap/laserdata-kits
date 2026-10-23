"""Shared settings for the card testing control lane kit: every card attempt reaches the card testing detector
in milliseconds, and every intervention it chooses reaches every payments API node before the business's next
attempt, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "payments-risk"
ATTEMPTS = "card-attempts"      # the lane: one record per card setup or payment attempt, keyed by business
CONTROLS = "attack-controls"    # the detector's interventions and easings, read by every payments API node
LOGS = "api-request-logs"       # a heavy, latency-tolerant stream on the same broker: API request logs
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {ATTEMPTS: 16, CONTROLS: 1, LOGS: 32}

SURFACES = ("checkout", "payment_element", "setup_intent")


def business(i: int) -> str:
    return f"acct_{i:07d}"


def part(b: str, partitions: int) -> int:
    # One partition per business, the same function in every script, so a business's attempts stay in order.
    return zlib.crc32(b.encode()) % partitions


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
