"""Shared settings for the live watch signals kit: what each profile is watching live on Disney+ and ESPN
reaches the live personalization features before that profile's next Live rail request, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "live-personalization"
WATCH = "watch-signals"          # the lane: each start, switch or stop of a live event, keyed by profile
RAILS = "live-rail-requests"     # the profile's next Live rail request, built from those features
CLIENT = "client-signals"        # a heavy, latency-tolerant stream on the same broker (playback beacons, analytics)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {WATCH: 16, RAILS: 16, CLIENT: 32}


def profile_id(n: int) -> str:
    return f"P{n:09d}"           # illustrative profile IDs (one account has several profiles)


def event_id(n: int) -> str:
    return f"LIVE-{n:04d}"       # illustrative live event IDs (games, matches, races airing now)


def part(profile: str, partitions: int) -> int:
    # One partition per profile, the same function in every script, so a profile's signals stay in order.
    return zlib.crc32(profile.encode()) % partitions


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
