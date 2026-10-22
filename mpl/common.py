"""Shared settings for the MPL player form lane: every finished game's result, as the player's resulting form
(rating, win or loss streak, games played), keyed by player, into every matchmaker in milliseconds, on Apache Iggy.

A message on the lane is the resulting form of one player after a game, never a delta: rating after the game,
streak (positive: wins in a row, negative: losses in a row), games played, the game it came from, a per-player
sequence number, and the time the game server created the message.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "matchmaking"
FORM = "player-form"             # the lane: one record per finished game per player, keyed by player, resulting form
REQUESTS = "match-requests"      # players tapping Play again, keyed by lobby (one matchmaker owns a lobby)
MATCHES = "matches"              # each matchmaker's decision, back to the game service, which checks it
APP_EVENTS = "app-events"        # a heavy, latency-tolerant neighbour on the same broker (clickstream, marketing)
PARTITIONS = {FORM: 16, REQUESTS: 8, MATCHES: 8, APP_EVENTS: 16}
RETENTION = {FORM: IggyExpiry.ExpireDuration(timedelta(days=1))}   # a day of results holds every active player's form
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

# Lobbies the kit's players queue in: casual skill games of the kind MPL lists (illustrative names).
GAMES = ("pool", "bowling", "block-puzzle", "21-puzzle", "baseball-star", "carrom", "ludo", "solitaire",
         "fruit-slice", "archery", "bubble-shooter", "knife-hit", "basketball", "snake", "word-hunt", "sudoku")
START_RATING = 1200              # a player with no form yet is matched at the starting rating


def part_of(key: str, partitions: int = PARTITIONS[FORM]) -> int:
    # The one partition function used everywhere: a player's results always land in one partition, in order.
    return zlib.crc32(key.encode()) % partitions


def player_id(i: int) -> str:
    return f"P{i:08d}"


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
                await c.create_topic(STREAM, t, PARTITIONS[t], message_expiry=RETENTION.get(t))
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
