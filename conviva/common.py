"""Shared settings for the live ad break lane kit: every ad event from a viewer session (break start, ad attempt,
impression, ad start failure, slate, break end) reaches the ad break checker in milliseconds, in order per
session, and every cohort alert (ad server x market) reaches ad ops while the break is still running, on Apache
Iggy."""
import asyncio
import gc
import json
import time
import zlib

STREAM = "ad-experience"
AD_EVENTS = "ad-events"             # the lane: ad events from each viewer session, keyed by session
ALERTS = "ad-alerts"                # the checker's output, read by every consumer (ad ops console, webhooks, agents)
HEARTBEATS = "session-heartbeats"   # full-census playback heartbeats: heavy, the neighbour on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {AD_EVENTS: 16, ALERTS: 1, HEARTBEATS: 16}

# Cohort dimensions (illustrative; yours come from session metadata). Conviva reports fill by market, device and
# ad server; the checker keys its cohorts by ad server x market and names the devices in each alert.
AD_SERVERS = ["ad-server-a", "ad-server-b", "ad-server-c"]
MARKETS = ["new-york", "los-angeles", "chicago", "dallas", "atlanta", "seattle"]
DEVICES = ["roku", "fire-tv", "apple-tv", "smart-tv", "ios", "android", "web"]

# Slot outcomes. An impression is an ad creative that started. The rest leave the slot unfilled:
RESPONSE_FAILURE = "response_failure"   # ad start failure: no usable response from the ad server (e.g. a timeout)
CREATIVE_FAILURE = "creative_media_failure"   # ad start failure: the creative failed to load or play
SLATE = "slate"                         # live server-side break: a slate played instead of an ad
UNFILLED = (RESPONSE_FAILURE, CREATIVE_FAILURE, SLATE)


def session(i: int) -> str:
    return f"sess-{i:08d}"


def part(key: str, partitions: int) -> int:
    # One partition per session, the same function in every script, so a session's ad events stay in order.
    return zlib.crc32(key.encode()) % partitions


def cohort_of(s: str):
    """The session's ad server, market and device. The same in every script, so the kit needs no lookup."""
    h = zlib.crc32(s.encode())
    return AD_SERVERS[h % 3], MARKETS[(h // 3) % 6], DEVICES[(h // 18) % 7]


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
