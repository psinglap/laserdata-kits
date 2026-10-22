"""Shared settings for the in-flight spend lane kit: every ad insertion's in-flight spend, keyed by ad group,
reaches the budget view of every ad server in milliseconds, on Apache Iggy, so ad groups stop at their daily
budget instead of serving free ads.

A message on the lane is one insertion's spend: the ad group, the budget day, and the writer's running
in-flight spend for that ad group in that day (the resulting total in micro-dollars, never a delta), with a
per-(writer, ad group) sequence number and the time it was published. The writer and the sequence together
are the event ID: a repeat (a client retry, an at-least-once redelivery, a replay) carries the same ID and
changes nothing.

In-flight spend follows the formula Pinterest published: price x impression_rate x action_rate, where the
action rate is the click probability for click-billed ads and 1 for impression-billed ads.
"""
import asyncio
import json
import random
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "pinterest-ads"
SPEND = "inflight-spend"            # the lane: one message per insertion, keyed by ad group
INSERTIONS = "ad-insertion-log"     # a heavy, latency-tolerant neighbour: full insertion and engagement logs
PARTITIONS = {SPEND: 8, INSERTIONS: 16}
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=48))   # two budget days, for rebuilds and replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once


def adgroup(i: int) -> str:
    return f"ag-{i:07d}"


def part(key: str, partitions: int) -> int:
    # The same function in every script, so an ad group's spend always lands on one partition, in order.
    return zlib.crc32(key.encode()) % partitions


def budget_day(day_s: float, now: float | None = None) -> int:
    # The budget day (a day in production; --day-s compresses it), derived from the clock by every script.
    return int((time.time() if now is None else now) // day_s)


def inflight_cost(i: int) -> int:
    """In-flight spend of one insertion of ad group i, in micro-dollars: price x impression_rate x action_rate.
    Derived from the ad group's index, so every ad server prices it the same way. Illustrative values."""
    r = random.Random(i)
    impression_rate = r.uniform(0.6, 0.9)          # share of insertions that become an impression
    if r.random() < 0.5:                           # impression-billed: CPM $2-$20, action rate 1
        return max(1, int(r.uniform(2, 20) / 1000 * 1e6 * impression_rate))
    ctr = r.uniform(0.005, 0.02)                   # click-billed: CPC $0.30-$2.00, action rate = click probability
    return max(1, int(r.uniform(0.3, 2.0) * 1e6 * impression_rate * ctr))


def daily_budget(i: int, lo: int, hi: int) -> int:
    """The day's spend limit for ad group i, in micro-dollars: room for lo..hi insertions. Illustrative."""
    return inflight_cost(i) * random.Random(i * 7 + 1).randint(lo, hi)


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
                await c.create_topic(STREAM, t, PARTITIONS[t], message_expiry=RETENTION)
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
