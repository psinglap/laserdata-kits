"""Shared settings for the Robinhood card state lane kit: every change to a card account (spendable balance and
card status) reaches every stand-in authorizer's cache in milliseconds, on Apache Iggy."""
import asyncio
import json
import time
import zlib

from apache_iggy import IggyClient

STREAM = "card-auth"
STATE = "card-state"             # the lane: one record per account change, keyed by account, resulting state
REQUESTS = "auth-requests"        # authorization requests, as the processor sends them, keyed by account
DECISIONS = "standin-decisions"  # each stand-in decision, with the state version it used, for the comparison
SETTLEMENT = "settlement-backfill"  # a heavy, latency-tolerant stream on the same broker (clearing records)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {STATE: 16, REQUESTS: 8, DECISIONS: 8, SETTLEMENT: 16}


def account(i: int) -> str:
    return f"A{i:08d}"


def part_of(k: str, partitions: int = PARTITIONS[STATE]) -> int:
    # The one partition function used everywhere: an account's changes always land in one partition, in order.
    return zlib.crc32(k.encode()) % partitions


def decide(avail: int, status: str, amount: int) -> bool:
    # The authorization rule both sides apply: the card is active and the account can cover the amount (cents).
    return status == "active" and amount <= avail


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topics(c: IggyClient, *topics):
    """Create the stream and topics if they are missing, so any script can start first."""
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    for t in topics:
        if await c.get_topic(STREAM, t) is None:
            try:
                await c.create_topic(STREAM, t, PARTITIONS[t])
            except RuntimeError:
                pass
    while not all([await c.get_topic(STREAM, t) for t in topics]):
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
