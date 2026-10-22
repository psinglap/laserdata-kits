"""Shared settings for the live scorecard lane: every score update of every live match to every scorecard node,
on Apache Iggy.

A message on the lane is one score update of one live match, shaped like the answer to FanCode's
scorecardPoll(matchId, lastKey): the match id, its sport, the match's own key (one more on every update),
what happened, and the resulting score of both sides (the score so far, never a delta), plus the time the
score feed created it. A node answers a poll from the newest message it holds for that match.
"""
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "fancode-live"
LANE = "scorecard"                   # one copy of every score update, every live match, every sport
NEIGHBOUR = "app-events"             # a busy neighbour: app events in batches, with a surge at a big match start
PARTITIONS = 4                       # a match always lands on one partition, so its updates stay in order
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))   # a match day, for rebuilds and replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

# The sports scorecardPoll answers for, and how often each one's score moves in real time (seconds per update).
SPORTS = {"cricket": 35, "kabaddi": 30, "football": 90, "tennis": 40, "basketball": 25, "hockey": 120}
MIX = ["cricket"] * 5 + ["kabaddi", "football", "tennis", "basketball", "hockey"]   # cricket draws most events


def partition_of(match: str) -> int:
    # The same function everywhere, so a match's updates always land on one partition, in order.
    return zlib.crc32(match.encode()) % PARTITIONS


def match_ids(n: int) -> list:
    return [f"FC{120001 + i}" for i in range(n)]


def sport_of(i: int) -> str:
    return MIX[i % len(MIX)]


def popularity(i: int) -> float:
    # One headline match (an India game) draws most viewers; a long tail of leagues draws the rest.
    return 1.0 / (i + 1) ** 1.1


def pack(match: str, sport: str, key: int, event: str, score: list, waited_ns: int = 0) -> bytes:
    # Created time = now, minus how long the update has already waited for its write.
    return json.dumps({"m": match, "sp": sport, "k": key, "e": event, "sc": score,
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
