"""Shared settings for the Meecom order-update lane kit: order notifications ahead of sale-day campaigns, on Apache Iggy."""
import asyncio
import json
import random
import time
import zlib

STREAM = "meecom"
ORDER_UPDATES = "order-updates"      # the lane: order updates your services push to Meecom
CAMPAIGN = "campaign-sends"          # promotional sends from campaign creators, on their own topic
RECEIPTS = "send-receipts"           # Meecom's "sent" events, the first step of the Sent -> Delivered -> Clicked -> Read funnel
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090"
PARTITIONS = {ORDER_UPDATES: 16, CAMPAIGN: 32, RECEIPTS: 8}

# The order states a shopper is told about, in order.
STATES = ("placed", "confirmed", "shipped", "out_for_delivery", "delivered")

# Channel mix from Meecom's published daily volumes: 10B notifications, of which
# 200M WhatsApp, 50M SMS and 40M email; the rest is push.
CHANNELS = (("push", 9_710), ("whatsapp", 200), ("sms", 50), ("email", 40))


def channel() -> str:
    r = random.randrange(10_000)
    for name, share in CHANNELS:
        if r < share:
            return name
        r -= share
    return "push"


def order_id(n: int) -> str:
    return f"MO{n:010d}"


def part(key: str, partitions: int) -> int:
    # One partition per order, the same function in every script, so an order's updates stay in order.
    return zlib.crc32(key.encode()) % partitions


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
