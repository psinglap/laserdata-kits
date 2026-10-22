"""Shared settings for the Autoplay lane: every listening action on Pocket FM reaches the listener's Autoplay
queue before the player asks it for the next show, on Apache Iggy."""
import asyncio
import json
import random
import time
import zlib

STREAM = "pocketfm-autoplay"
ACTIONS = "listening-actions"    # the lane: play, episode complete, unlock, skip, show complete, keyed by user
AUTOPLAY = "autoplay-requests"   # the player asking for the next show at the end of content
ANALYTICS = "listening-analytics"  # a heavy, latency-tolerant stream on the same broker (playtime pings)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {ACTIONS: 16, AUTOPLAY: 16, ANALYTICS: 32}

# The actions Pocket FM's posts name: what a listener plays, completes or skips (Autoplay post, Jun 2025), and
# the core actions that unlock the next episode: coins, an ad, a subscription (Weighted Playtime post, Apr 2026).
KINDS = ["play", "episode_complete", "unlock", "skip", "show_complete"]
UNLOCKS = ["coins", "ad", "subscription"]

CATALOG = 20_000                 # illustrative show IDs; Pocket FM lists over 770,000 audio series
QUEUE_LEN = 10                   # shows in a listener's daily Autoplay queue (illustrative)
KEEP_DROPPED = 32                # shows dropped this session, kept per listener: constant space per listener


def user_id(n: int) -> str:
    return f"U{n:09d}"           # illustrative user IDs


def show_id(n: int) -> str:
    return f"S{n:06d}"           # illustrative show IDs


def part(user: str, partitions: int) -> int:
    # One partition per listener, the same function in every script, so a listener's actions stay in order.
    return zlib.crc32(user.encode()) % partitions


def daily_queue(user: str) -> list:
    """Stands in for the listener's daily playlist from the hybrid model (EASE plus the ensemble), which
    Pocket FM keeps in Redis. Here it is a fixed function of the user, so every script sees the same queue."""
    r = random.Random(zlib.crc32(user.encode()))
    return [show_id(n) for n in r.sample(range(CATALOG), QUEUE_LEN)]


def fold(q, user: str, sess: str, kind: str, show: str):
    """Apply one listening action to a listener's Autoplay queue state and return the new state.

    q is [session, show playing now, [shows dropped this session], daily queue]. A skipped or completed
    show leaves the queue; the show playing now is never offered as the next one. The app and the queue
    service fold the same actions in the same order, so they agree on the next show."""
    if q is None or q[0] != sess:
        q = [sess, "", [], daily_queue(user)]
    if kind == "play":
        q[1] = show
    elif kind in ("skip", "show_complete"):
        if show not in q[2]:
            q[2].append(show)
            del q[2][:-KEEP_DROPPED]
        if q[1] == show:
            q[1] = ""
    return q                      # episode_complete and unlock keep the show playing; they move nothing here


def head(q) -> str:
    """The show Autoplay starts next: the first in the daily queue that is not playing and not dropped."""
    if not q:
        return ""
    for s in q[3]:
        if s != q[1] and s not in q[2]:
            return s
    return ""                     # queue used up: the next daily build refills it


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def ensure_topics(client, *topics):
    """Create the stream and topics if they are missing, so any script can start first."""
    if await client.get_stream(STREAM) is None:
        try:
            await client.create_stream(STREAM)
        except RuntimeError:
            pass                                    # created by another script in the meantime
    for t in topics:
        if await client.get_topic(STREAM, t) is None:
            try:
                await client.create_topic(STREAM, t, PARTITIONS[t])
            except RuntimeError:
                pass
    while not all([await client.get_topic(STREAM, t) for t in topics]):
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
