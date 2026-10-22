"""Shared settings for the Dhan live order update lane kit: every order status change from DEXT, keyed by client ID,
to every order update gateway (the Dhan app, web.dhan.co and DhanHQ API sockets) in milliseconds, and replayed from
the log when a socket reconnects, on Apache Iggy.

An update on the lane carries the client ID, a per-client sequence number, the order and the order's resulting
state (status, quantity, traded quantity, price, average traded price, Super Order leg), never only a delta, plus
the time DEXT created it. Status values and leg numbers follow DhanHQ's Live Order Update docs: TRANSIT, PENDING,
REJECTED, CANCELLED, TRADED, EXPIRED; LegNo 1 entry, 2 stop-loss, 3 target.
"""
import asyncio
import json
import time
import zlib

from apache_iggy import IggyClient

STREAM = "dext"
UPDATES = "order-updates"        # the lane: one record per order status change, keyed by client ID
EXPORT = "compliance-export"     # a heavy, latency-tolerant neighbour (trade records for contract notes and reporting)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {UPDATES: 4, EXPORT: 8}
# (exchange segment, symbol, lot size, reference price). Illustrative instruments across the segments DhanHQ lists.
INSTRUMENTS = [("NSE_FNO", "NIFTY 24500 CE", 75, 182.0), ("NSE_FNO", "NIFTY 24400 PE", 75, 141.0),
               ("NSE_FNO", "BANKNIFTY 52000 CE", 35, 410.0), ("NSE_FNO", "SENSEX 80000 CE", 20, 236.0),
               ("NSE_EQ", "RELIANCE", 1, 2870.0), ("NSE_EQ", "HDFCBANK", 1, 1712.0), ("NSE_EQ", "IDEA", 1, 13.2),
               ("NSE_EQ", "TATAMOTORS", 1, 968.0), ("BSE_EQ", "INFY", 1, 1844.0), ("MCX_COMM", "CRUDEOIL", 100, 6120.0),
               ("NSE_FNO", "FINNIFTY 23800 CE", 65, 96.0), ("NSE_EQ", "SBIN", 1, 812.0)]


def client(i: int) -> str:
    # A Dhan client ID: ten digits, as in DhanHQ's samples ("1000000001"). Illustrative.
    return str(1000000001 + i)


def part_of(k: str, partitions: int = PARTITIONS[UPDATES]) -> int:
    # The one partition function used everywhere: a client's updates always land in one partition, in order.
    return zlib.crc32(k.encode()) % partitions


def gateway_of(k: str, gateways: int) -> int:
    # Which gateway a client's socket is on. The load balancer picks it at connect; any gateway must be able to
    # serve any client, so every gateway reads every partition.
    return zlib.crc32(b"gw" + k.encode()) % gateways


def order_no(n: int) -> str:
    # A 13-digit Dhan order number, like "1124091136546" in DhanHQ's sample. Illustrative.
    return str(1126102200000 + n)


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
