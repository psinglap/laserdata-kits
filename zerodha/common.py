"""Shared settings for the tick lane kit: exchange ticks to every ticker node, on Apache Iggy.

A tick on the lane is Kite's own 44-byte `quote` packet (big-endian int32s, prices in paise)
followed by the feed's per-instrument sequence number and the time the feed handler created it.
"""
import struct
import time
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "market-data"
TICKS = "ticks"                      # one partition per exchange segment, so each instrument stays in order
POSTBACKS = "order-postbacks"        # a busy neighbour, for the isolation test
SEGMENTS = ["NSE", "NFO", "BSE", "MCX"]
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))   # a trading day, for rebuilds and replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"

# token, last price, last qty, avg price, volume, buy qty, sell qty, open, high, low, close | seq, created ns
QUOTE = struct.Struct(">11i")
TICK = struct.Struct(">11iIq")
SIZE = TICK.size                     # 56 bytes


# Kite instrument tokens carry the exchange segment in their low byte (nse 1, nfo 2, cds 3, bse 4,
# bfo 5, bcd 6, mcx 7, mcxsx 8, indices 9, nco 12, as in Kite's ticker clients). The partition is the segment.
PARTITION_OF_SEGMENT = {1: 0, 3: 0, 9: 0, 2: 1, 4: 2, 5: 2, 6: 2, 7: 3, 8: 3, 12: 3}


def segment_of(token: int) -> int:
    # The same function everywhere, so an instrument's ticks always land on one partition, in order.
    return PARTITION_OF_SEGMENT.get(token & 0xFF, (token & 0xFF) % len(SEGMENTS))


def pack(token, ltp, qty, avg, volume, buy, sell, o, h, l, c, seq, waited_ns=0) -> bytes:
    # Created time = now, minus how long the tick has already waited for its write.
    return TICK.pack(token, ltp, qty, avg, volume, buy, sell, o, h, l, c, seq, time.time_ns() - waited_ns)


def unpack(b: bytes):
    """Returns (token, last price in paise, seq, created ns)."""
    t = TICK.unpack_from(b)
    return t[0], t[1], t[11], t[12]


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
        out = {"n": len(self.samples), "p50": pick(0.5), "p99": pick(0.99), "p999": pick(0.999), "max": s[-1]}
        self.samples = []
        return out


def instruments(n: int) -> list:
    # Illustrative tokens in Kite's layout: exchange token << 8 | segment. Mostly F&O contracts,
    # then NSE and BSE cash, a few MCX contracts.
    mix = [2] * 10 + [1] * 5 + [4] * 4 + [7]
    return [((1000 + i) << 8) | mix[i % len(mix)] for i in range(n)]


def popular(rng, n: int) -> int:
    # A few instruments (index futures, large caps) tick far more often than the long tail.
    return int(n * rng.random() ** 3)
