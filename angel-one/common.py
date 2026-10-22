"""Shared settings for the candle lane kit: price ticks into the chart candle builders, on Apache Iggy.

A tick carries what Angel One's "Charting at Angel One" post lists for a tick (scrip code,
timestamp, price, total volume for the day), plus a per-scrip sequence number. The timestamp is
when the price capture engine stamped the tick; the candle builder meters it against its own
clock, the same delay Angel One already meters (p50 ~550 ms, p99 ~1,000 ms on a typical day).
"""
import struct
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "charting"
TICKS = "price-ticks"                 # keyed by scrip code, so each scrip's ticks stay in order
ORDERS = "order-events"               # a busy neighbour, for the isolation test
PARTITIONS = 4
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))   # a trading day, for rebuilds and DR replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"

# scrip code, timestamp ns, price (paise), total volume for the day, per-scrip seq
TICK = struct.Struct(">iqiqI")
SIZE = TICK.size                      # 28 bytes


def part(scrip: int) -> int:
    # The same function everywhere (capture, bridge, readers), so a scrip's ticks always share one partition.
    return zlib.crc32(scrip.to_bytes(4, "big")) % PARTITIONS


def pack(scrip, ts_ns, price, total_volume, seq) -> bytes:
    return TICK.pack(scrip, ts_ns, price, total_volume, seq)


def unpack(b: bytes):
    """Returns (scrip code, timestamp ns, price in paise, total volume, seq)."""
    return TICK.unpack_from(b)


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topic(c: IggyClient, topic: str, partitions: int):
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    if await c.get_topic(STREAM, topic) is None:
        try:
            await c.create_topic(STREAM, topic, partitions, message_expiry=RETENTION)
        except RuntimeError:
            pass


class Window:
    def __init__(self):
        self.samples = []

    def add(self, v):
        self.samples.append(v)

    def flush(self):
        s = sorted(self.samples) or [0.0]
        pick = lambda q: s[min(len(s) - 1, int(q * len(s)))]
        out = {"n": len(self.samples), "p50": pick(0.5), "p95": pick(0.95), "p99": pick(0.99),
               "p999": pick(0.999), "max": s[-1]}
        self.samples = []
        return out


def scrips(n: int) -> list:
    # Illustrative scrip codes: index and stock futures and options make up most of a broker's
    # instruments, then NSE and BSE cash and MCX contracts. Replace with your own scrip master.
    return [10_000 + i for i in range(n)]


def popular(rng, n: int) -> int:
    # A few scrips (index options near the money, large caps) tick far more often than the long tail.
    return int(n * rng.random() ** 3)


def hhmmss(ns: int) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ns / 1e9))
