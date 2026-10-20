"""Shared settings for the vehicle state lane kit: the signals the Rivian app shows, from every vehicle's
five-second report into the latest-state service the app reads, in milliseconds and in order per vehicle,
on Apache Iggy."""
import asyncio
import gc
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyExpiry

STREAM = "vehicle-cloud"
STATE = "app-vehicle-state"      # the lane: app-facing signals from each vehicle report, keyed by vehicle
READS = "app-reads"              # what the latest-state service served to each app open (for the truth check)
SIGNALS = "vehicle-signals"      # the full signal set of every report: big, steady, read by analytics (the neighbour)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {STATE: 16, READS: 1, SIGNALS: 16}
RETENTION = {STATE: IggyExpiry.ExpireDuration(timedelta(hours=24)),     # a day of state changes to rebuild from
             READS: IggyExpiry.ExpireDuration(timedelta(hours=1)),
             SIGNALS: IggyExpiry.ExpireDuration(timedelta(hours=6))}

# The signals the app shows (illustrative names; yours come from your signal catalogue). Each report carries
# the resulting value of every one of them, never a delta.
APP_SIGNALS = ("soc", "range_mi", "charge", "charge_kw", "locked", "closures", "cabin_c", "gear", "odo_mi",
               "lat", "lon")


def vehicle(i: int) -> str:
    return f"7PDS{i:08d}"        # an illustrative vehicle ID, not a real VIN


def part(key: str, partitions: int) -> int:
    # One partition per vehicle, the same function in every script, so a vehicle's reports stay in order.
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
                await client.create_topic(STREAM, t, PARTITIONS[t], message_expiry=RETENTION[t])
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
