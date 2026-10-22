"""Shared settings for the rider alert lane kit: charging, parked-movement and crash events from every
connected scooter reach the alert service in milliseconds, in order per scooter, while the backlog a scooter's
Cloud Connect module flushes after a network drop is filed to ride history on its own topic, on Apache Iggy."""
import asyncio
import gc
import json
import time
import zlib

STREAM = "scooter-cloud"
LANE = "rider-events"               # the lane: live rider-facing events, keyed by scooter
BACKLOG = "reconnect-backlog"       # events a scooter buffered while offline, flushed on reconnect: for ride history
ALERTS = "rider-alerts"             # the alert service's output, read by every notifier (app push, SMS, support)
TELEMETRY = "vehicle-telemetry"     # full sensor packets: big, steady, read by analytics (the neighbour)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {LANE: 16, BACKLOG: 8, ALERTS: 1, TELEMETRY: 8}

# Event kinds (illustrative names; yours come from the vehicle's event catalogue). Every event carries the
# scooter's whole current state (charging, state of charge, parked or riding, position), never a delta.
CHARGE_START = "charge_started"
CHARGE_DONE = "charge_complete"
MOVED = "parked_movement"           # movement while parked and locked (theft or tow)
CRASH = "crash"                     # a crash or sudden impact: emergency contacts get the live location
QUIET = ("ride_start", "ride_end", "soc")      # state changes that update the app but raise no alert
ALERT_KINDS = (CHARGE_START, CHARGE_DONE, MOVED, CRASH)

# How old (by the scooter's own clock) an event may be and still be pushed as news. Older ones are history:
# the ride stats and the app's timeline get them, the rider's phone does not buzz. A crash is always sent,
# however late, and marked as delayed. 120 s is illustrative; set it per alert kind to yours.
FRESH_S = 120
ALWAYS_SEND = {CRASH}


def scooter(i: int) -> str:
    return f"ath-{i:07d}"           # an illustrative scooter ID, not a real VIN


def part(key: str, partitions: int) -> int:
    # One partition per scooter, the same function in every script, so a scooter's events stay in order.
    return zlib.crc32(key.encode()) % partitions


def route(kind: str, vt_ns: int, now_ns: int) -> str:
    """Where the ingest step puts an event: on the live lane if it is fresh (or a crash), otherwise on the
    reconnect backlog. The scooter's own clock decides, not arrival order, so a flush of buffered events never
    queues ahead of live ones."""
    if kind in ALWAYS_SEND or now_ns - vt_ns <= FRESH_S * 1e9:
        return LANE
    return BACKLOG


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
