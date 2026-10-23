"""Shared settings for the Alpaca partner trade event lane kit: every trade event the OMS writes (new, partial_fill,
fill, canceled, replaced, a rare trade_correct), into one log per broker partner, read by the Broker API's event
stream gateways and pushed to each partner in milliseconds. A partner that reconnects resumes from the last
event_id it processed, and a partner that reads slowly falls behind in the log instead of losing events.

An event carries the partner (correspondent), the account ID, a per-account sequence number, a ULID event_id, and
the order's resulting state (status, qty, filled_qty, filled_avg_price), never only a delta, plus the time the OMS
created it. Event names follow Alpaca's trade_updates docs and Broker API trade events SSE docs.
"""
import asyncio
import json
import os
import time
import uuid
import zlib

from apache_iggy import IggyClient

STREAM = "broker-events"
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

# Illustrative broker partners (correspondents) and the share of today's trade events each one generates. One log
# (topic) per partner, so one partner's stream never shares a read path with another's. A large partner gets more
# partitions: Alpaca's Activity SSE docs offer "sharded streaming" to customers with large event volumes.
# (code, what the partner runs, share of events, partitions)
PARTNERS = [("corr-01", "robo-advisor (fractional rebalances)", 0.40, 2),
            ("corr-02", "neobank investing tab", 0.18, 1),
            ("corr-03", "trading app", 0.14, 1),
            ("corr-04", "wealth platform", 0.12, 1),
            ("corr-05", "crypto and stocks wallet", 0.09, 1),
            ("corr-06", "advisor dashboard", 0.07, 1)]
CODES = [p[0] for p in PARTNERS]
PARTS = {p[0]: p[3] for p in PARTNERS}
# (symbol, reference price). US equities and ETFs traded in fractional shares, and two crypto pairs.
SYMBOLS = [("AAPL", 228.4), ("NVDA", 131.2), ("TSLA", 251.9), ("SPY", 571.3), ("MSFT", 418.6), ("AMZN", 186.5),
           ("QQQ", 489.7), ("VTI", 287.1), ("GOOGL", 163.8), ("META", 576.9), ("BTC/USD", 67250.0), ("ETH/USD", 2640.0)]


def topic(code: str) -> str:
    return f"trade-events-{code}"


def account(code: str, i: int) -> str:
    # A Broker API account_id is a UUID. Illustrative, stable per partner and index.
    return str(uuid.UUID(int=(zlib.crc32(code.encode()) << 96) | (i + 1)))


def part_of(acct: str, partitions: int) -> int:
    # The one partition function used everywhere: an account's events always land in one partition, in order
    # (Alpaca's SSE docs guarantee per-account order, and nothing across accounts).
    return zlib.crc32(acct.encode()) % partitions


_B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def ulid(ms: int) -> str:
    # 48-bit millisecond time + 80 random bits, Crockford base32, like Alpaca's event_id.
    n = (ms << 80) | int.from_bytes(os.urandom(10), "big")
    return "".join(_B32[(n >> (5 * i)) & 31] for i in range(25, -1, -1))


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


async def ensure_topics(c: IggyClient, codes=CODES):
    """Create the stream and the partners' topics if they are missing, so any script can start first."""
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    for code in codes:
        if await c.get_topic(STREAM, topic(code)) is None:
            try:
                await c.create_topic(STREAM, topic(code), PARTS[code])
            except RuntimeError:
                pass
    while not all([await c.get_topic(STREAM, topic(code)) for code in codes]):
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
