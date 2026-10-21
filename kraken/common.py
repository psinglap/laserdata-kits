"""Shared settings for the Kraken execution report lane kit: every order event from the spot trading engine,
keyed by account, to every FIX and WebSocket gateway in milliseconds, and replayed from the log by sequence
number when a session reconnects, on Apache Iggy.

An execution report on the lane carries the account, its per-account sequence number, the order and the
order's resulting state (exec_type, order_status, quantity, filled quantity, price), never only a delta, plus
the time the engine created it. Field names follow Kraken's WebSocket v2 executions channel.
"""
import asyncio
import json
import time
import zlib

from apache_iggy import IggyClient

STREAM = "spot-trading"
REPORTS = "execution-reports"    # the lane: one record per order event, keyed by account
EXPORT = "ledger-export"         # a heavy, latency-tolerant neighbour (ledger entries for statements and tax)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {REPORTS: 4, EXPORT: 8}
PAIRS = ["BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "BTC/EUR", "ETH/EUR", "USDT/USD", "DOGE/USD", "ADA/USD",
         "LINK/USD", "DOT/USD", "LTC/USD"]


def account(i: int) -> str:
    return f"AC{i:07d}"


def part_of(k: str, partitions: int = PARTITIONS[REPORTS]) -> int:
    # The one partition function used everywhere: an account's reports always land in one partition, in order.
    return zlib.crc32(k.encode()) % partitions


def gateway_of(k: str, gateways: int) -> int:
    # Which gateway an account's session is logged on to. In production the load balancer picks the gateway at
    # logon; any gateway must be able to serve any account, so every gateway reads every partition.
    return zlib.crc32(b"gw" + k.encode()) % gateways


def order_id(n: int) -> str:
    # Kraken-style order id (three dash-separated groups), illustrative only.
    a = "ABCDEFGHJKLMNPQRSTUVWXYZ234567"
    s = "".join(a[(n * 7919 + i * 104729) % len(a)] for i in range(16))
    return f"O{s[:5]}-{s[5:10]}-{s[10:16]}"


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
