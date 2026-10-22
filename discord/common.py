"""Shared settings for the Osprey action lane kit.

Osprey actions (the events Discord's services send to its safety rules engine: a message sent, a
login attempt, an identity change) go to one Apache Iggy topic, keyed by user ID: every action of
one user lands on the same partition, in order. Each rules worker owns a fixed set of partitions and
reads them directly, so one user's actions are only ever evaluated by one worker, one after another,
and the label one action adds is in place before that user's next action is evaluated. Every
execution result (verdict, labels added, effects) goes to an execution-results topic, keyed the same
way, which the effect side (here effects_audit.py) reads.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "osprey"
ACTIONS_TOPIC = "actions-input"                    # async actions, keyed by user
RESULTS_TOPIC = "execution-results"                # verdicts, labels and effects, keyed by user
INDEX_STREAM = "search-index"                      # the bulk neighbour next door (reindex_backfill.py)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))          # a day of actions to replay or investigate

# Action types named in Discord's Osprey post (send_message, user_login_attempted, identity_change),
# plus guild_join; the mix is illustrative.
ACTIONS = ("user_login_attempted", "send_message", "identity_change", "guild_join")
SPAM_LABEL = "likely_spammer"


def user_id(n: int) -> str:
    # Discord-style snowflake (illustrative): a 19-digit ID per user.
    return str(1_100_000_000_000_000_000 + n * 7919)


def part(user: str, partitions: int) -> int:
    # The same function everywhere: one user, one partition, one owner.
    return zlib.crc32(user.encode()) % partitions


def owner(pid: int, workers: int) -> int:
    # Static ownership: partition p belongs to worker p % workers. No lock, no lease, no rebalance.
    return pid % workers


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


async def ensure_topic(c: IggyClient, stream: str, topic: str, partitions: int, expiry=RETENTION):
    """Create the stream and topic if they are missing, so any script can start first."""
    if await c.get_stream(stream) is None:
        try:
            await c.create_stream(stream)
        except RuntimeError:
            pass                                    # created by another process in the meantime
    if await c.get_topic(stream, topic) is None:
        try:
            await c.create_topic(stream, topic, partitions, message_expiry=expiry)
        except RuntimeError:
            pass
    while not await c.get_topic(stream, topic):
        await asyncio.sleep(0.2)


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
