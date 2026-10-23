"""Shared settings for the graph link lane kit: every login's and profile change's account-to-asset links
reach PayPal's real-time risk graph before the two-hop check on that account's next payment, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "risk-graph"
LINKS = "asset-links"           # the lane: each account-to-asset link (device, IP, VID, address), keyed by asset
CHECKS = "payment-checks"       # the two-hop graph check on an account's next payment, keyed by account
TRACKING = "tracking-events"    # a heavy, latency-tolerant first-party tracking stream on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {LINKS: 16, CHECKS: 16, TRACKING: 32}

# A slower write path the checks are compared against. PayPal's graph post (Dec 2021) asks for
# "Seconds level data freshness" and says the graph "can be updated with the latest data in a second-level gap".
# One second is the kinder end of that; set --slow-path-s to the freshness you measure.
SLOW_PATH_S = 1.0


def account_id(n: int) -> str:
    return f"A{n:010d}"                     # illustrative account IDs


def part(key: str, partitions: int) -> int:
    # One partition per key, the same function in every script, so one asset's links stay in order.
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
