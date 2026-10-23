"""Shared settings for the camera partition alarm lane kit: every camera detection, access-control disarm, arm
and panic press for an alarm partition reaches the alarm state service in milliseconds, in order per partition,
and every raised alarm reaches the response platform at once, on Apache Iggy."""
import asyncio
import gc
import json
import time
import zlib

STREAM = "new-alarms"
EVENTS = "camera-partition-events"  # the lane: detections, arms, disarms and panic presses, keyed by alarm partition
RAISED = "alarms-raised"            # the alarm state service's output, read by every responder
ANALYTICS = "camera-analytics"      # people and vehicle metadata for search: heavy, latency-tolerant
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {EVENTS: 16, RAISED: 1, ANALYTICS: 16}

# Event kinds on the lane (the shape of your records goes in kafka_bridge.to_event()).
DETECT = "detect"                   # a camera trigger: person detection, line crossing or loitering
ARM = "arm"                         # badge twice within 5 s, a schedule, Command or the keypad
DISARM = "disarm"                   # an access-granted event at a configured door, Command or the keypad
PANIC = "panic"                     # a digital panic button in Command or the app: raised at once
TRIGGERS = ("person", "line_crossing", "loitering")
DISARM_BY = ("badge", "face_unlock", "mobile_nfc", "entry_code", "bluetooth_unlock")

# Alarm kinds on alarms-raised.
INTRUSION = "intrusion"             # a trigger in an armed partition, not disarmed before its entry delay ran out
SILENT = "panic"                    # a panic press


def partition(i: int) -> str:
    """An alarm partition: a group of cameras armed and disarmed together (two per site here; illustrative)."""
    return f"site-{i // 2:06d}/cp{i % 2 + 1}"


def part(key: str, partitions: int) -> int:
    # One log partition per alarm partition, the same function in every script, so its events stay in order.
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
