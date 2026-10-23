"""Shared settings for the story SLO lane kit: every trigger, step and outcome event of a user journey (a story),
keyed by story key, reaches the story judge in milliseconds; each story is judged the moment its last required
outcome lands, or at its deadline, and every SLO alert (story x surface) reaches on-call while the incident is still
young, on Apache Iggy."""
import asyncio
import gc
import json
import time
import zlib

STREAM = "e2e-slo"
STORY_EVENTS = "story-events"       # the lane: story events from clients and services, keyed by story key
VERDICTS = "slo-alerts"             # the judge's output, read by every consumer (pager, Datadog, incident bot)
CLIENT_EVENTS = "client-events"     # the rest of client telemetry: heavy, the neighbour on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {STORY_EVENTS: 16, VERDICTS: 1, CLIENT_EVENTS: 16}

SURFACES = ["ios", "android", "web"]

# Story schemas (illustrative; yours live in the story definitions you already encode as protobuf). A story starts
# with its trigger, may pass steps, and succeeds only if every required outcome arrives before the deadline. An
# expected-failure role (a product rule, such as a seller blocking a user) ends the story without counting it.
STORIES = {
    "livestream_join": {
        "trigger": "livestream_tile_tap",
        "steps": ["join_authorized"],
        "outcomes": ["video_playing", "auction_shown", "chat_visible"],
        "expected": ["seller_blocked"],
    },
    "place_bid": {
        "trigger": "bid_tap",
        "steps": ["bid_accepted"],
        "outcomes": ["bid_placed_shown"],
        "expected": ["auction_ended"],
    },
}


def part(key: str, partitions: int) -> int:
    # One partition per story, the same function in every script, so all events of a story reach one judge.
    return zlib.crc32(key.encode()) % partitions


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


class Stalls:
    """Longest garbage-collection pause and longest event-loop stall since the last read, in ms. Printed next to
    the latency, so a slow second can be traced to this process (or ruled out) rather than guessed at."""

    def __init__(self):
        self.gc = self.loop = 0.0
        self._t = 0.0
        gc.callbacks.append(self._cb)

    def _cb(self, phase, info):
        if phase == "start":
            self._t = time.perf_counter()
        else:
            self.gc = max(self.gc, (time.perf_counter() - self._t) * 1000)

    async def watch(self):
        while True:
            t = time.perf_counter()
            await asyncio.sleep(0.005)
            self.loop = max(self.loop, (time.perf_counter() - t - 0.005) * 1000)

    def read(self):
        out = f"stall gc {self.gc:.0f}ms loop {self.loop:.0f}ms"
        self.gc = self.loop = 0.0
        return out
