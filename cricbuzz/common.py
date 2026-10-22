"""Shared settings for the ball-by-ball commentary lane: every commentary line of every live match, and every
correction to a line already published, from the commentary desk to every commentary node and to match alerts,
on Apache Iggy.

A message on the lane is one entry of one live match's commentary, written once by the desk:
  * ball:  a new line for one delivery (over.ball, what happened, the text) with the resulting score;
  * edit:  a correction to a line already published, with that line's id and a higher version (a boundary
           re-scored as leg byes, a wicket overturned on review) and the resulting score;
  * over:  the end-of-over summary.
Every message carries the match's own sequence (one more per message, so a gap is visible), the line id and
version, and the resulting score of the batting side (the score so far, never a delta), plus the time the desk
created it. A reader applies a line only if its version is newer than the one it holds.
"""
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "cricbuzz-live"
LANE = "commentary"                  # one copy of every commentary line and correction, every live match
SENT = "alerts-sent"                 # what match alerts has already sent, so a restart never sends one twice
NEIGHBOUR = "impressions"            # a busy neighbour: ad impressions, with a surge at the innings break
PARTITIONS = 4                       # a match always lands on one partition, so its lines stay in order
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))   # a match day, for rebuilds and replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

# Formats on the live list at once, and the seconds between deliveries in real time (over rate plus breaks).
FORMATS = {"T20": (40, 120), "ODI": (45, 300), "Test": (45, None)}   # (s per ball, legal balls per innings)
MIX = ["T20"] * 3 + ["ODI", "Test"]  # T20 leagues and internationals fill most of the list


def partition_of(match: str) -> int:
    # The same function everywhere, so a match's lines always land on one partition, in order.
    return zlib.crc32(match.encode()) % PARTITIONS


def match_ids(n: int) -> list:
    return [f"CB{110001 + i}" for i in range(n)]


def format_of(i: int) -> str:
    return MIX[i % len(MIX)]


def pack(match: str, seq: int, kind: str, line: int, ver: int, over: str, event: str, text: str, score: list,
         waited_ns: int = 0) -> bytes:
    # Created time = now, minus how long the line has already waited for its write.
    return json.dumps({"m": match, "s": seq, "k": kind, "l": line, "v": ver, "o": over, "e": event, "t": text,
                       "sc": score, "ns": time.time_ns() - waited_ns}, separators=(",", ":")).encode()


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
