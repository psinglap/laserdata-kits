"""Shared settings for the fuel alert lane kit: every fuel-sensor ping from a truck's GPS device reaches the fuel
rules in milliseconds, in order per device, and every drain alert reaches the truck owner while the fuel is still
being drained, on Apache Iggy."""
import asyncio
import gc
import json
import time
import zlib

STREAM = "telematics"
PINGS = "fuel-pings"                # the lane: pings with fuel level, ignition, speed and position, keyed by device
ALERTS = "fuel-alerts"              # the fuel rules' output, read by every notifier (owner app, dashboard, SMS)
HISTORY = "history-pings"           # pings a device buffered in low network, uploaded on reconnect: heavy, not urgent
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {PINGS: 16, ALERTS: 1, HISTORY: 16}

# Event kinds (illustrative; your rule names go here).
DRAIN = "fuel_drain"                # fuel level falling while the truck is parked with the ignition off
REFUEL = "refuel"                   # fuel level rising at a stop: recorded, never an alarm

DRAIN_L = 15.0                      # litres below the stop's high mark before a drain alert (illustrative)
REFUEL_L = 20.0                     # litres above the stop's low mark before a refuel is recorded (illustrative)
SMOOTH = 3                          # median of the last 3 readings: one noisy reading never raises an alert


def device(i: int) -> str:
    return f"gps-{i:07d}"


def part(key: str, partitions: int) -> int:
    # One partition per device, the same function in every script, so a truck's pings stay in order.
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
