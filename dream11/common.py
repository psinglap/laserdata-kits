"""Shared settings for the live points lane: ball-by-ball fantasy points to every points node, on Apache Iggy.

A message on the lane is one ball of one live match: the match id, the match's own sequence
number, the over, what happened, and the resulting fantasy points of every player that ball
touched (totals so far, never a delta), plus the time the scoring feed created it.
"""
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "fantasy-live"
LANE = "match-points"                # one copy of every ball, every live match
SURGE = "team-saves"                 # a busy neighbour: team saves and contest joins after the toss
PARTITIONS = 4                       # a match always lands on one partition, so its balls stay in order
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))   # a match day, for rebuilds and replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PLAYERS = 22                         # both playing XIs; players 0-10 are side A, 11-21 side B


def partition_of(match: str) -> int:
    # The same function everywhere, so a match's balls always land on one partition, in order.
    return zlib.crc32(match.encode()) % PARTITIONS


def match_ids(n: int) -> list:
    return [f"M{i + 1:03d}" for i in range(n)]


def popularity(i: int) -> float:
    # A few matches (an IPL night game) draw far more teams than the long tail of leagues.
    return 1.0 / (i + 1) ** 0.9


def pack(match: str, seq: int, over: str, event: str, points: list, waited_ns: int = 0) -> bytes:
    # Created time = now, minus how long the ball has already waited for its write.
    return json.dumps({"m": match, "s": seq, "o": over, "e": event, "p": points,
                       "ns": time.time_ns() - waited_ns}, separators=(",", ":")).encode()


def unpack(b: bytes) -> dict:
    return json.loads(b)


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
