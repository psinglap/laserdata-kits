"""Shared settings for the key-play alerts lane: every pitch of every live pro baseball game, every key play the
moment model flags, every clip the cloud DVR cuts for it, and every replay review of it, to Game Alerts, on Apache
Iggy.

A message on the lane is one entry of one live game, written in order per game:
  * pitch:  one pitch, with the result when it ends a plate appearance, and the resulting score (the score so far,
            never a delta);
  * key:    a key play the moment model flagged (a home run, a go-ahead hit, a game-tying hit), with its play id,
            the team it is good news for, and the score after it;
  * clip:   the clip of that play is cut and playable from the cloud DVR recording (clip id, length);
  * review: a replay review of that play ended, upheld or overturned.
Every message carries the game's own sequence (one more per message, so a gap is visible), the play id where it
has one, and the time it was created. Game Alerts pushes a key-play alert only once its clip is playable, so a tap
on the alert always opens the play, and never pushes one whose call was overturned before the clip was ready.
"""
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "fubo-live"
LANE = "game-moments"                # one copy of every pitch, key play, clip and review, every live game
SENT = "alerts-sent"                 # what Game Alerts has already pushed, so a restart never pushes one twice
NEIGHBOUR = "viewing-events"         # a busy neighbour: player heartbeats, with a surge at first pitch
PARTITIONS = 4                       # a game always lands on one partition, so its moments stay in order
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))   # a game day, for rebuilds and replay
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

TEAMS = [f"T{i:02d}" for i in range(1, 31)]   # 30 clubs; names are illustrative


def partition_of(game: str) -> int:
    # The same function everywhere, so a game's moments always land on one partition, in order.
    return zlib.crc32(game.encode()) % PARTITIONS


def game_ids(n: int) -> list:
    return [f"G{260901 + i}" for i in range(n)]


def teams_of(i: int) -> tuple:
    # Away and home: a full slate is 15 games with every team playing; more games than that repeat the pairings.
    k = (i * 2) % len(TEAMS)
    return TEAMS[k], TEAMS[(k + 1 + (i // 15)) % len(TEAMS)]


def pack(game: str, seq: int, kind: str, play: int, body: dict, waited_ns: int = 0) -> bytes:
    # Created time = now, minus how long the entry has already waited for its write.
    return json.dumps({"g": game, "s": seq, "k": kind, "p": play, **body, "ns": time.time_ns() - waited_ns},
                      separators=(",", ":")).encode()


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
