"""Shared settings for the dock scan lane kit: every load scan, plan revision and seal at a hub or gateway dock,
keyed by outbound trip, reaches the load check in milliseconds, and every misload reaches the loader's handheld
before the next bag and before the trailer is sealed, on Apache Iggy."""
import asyncio
import gc
import json
import time
import zlib

STREAM = "hub-ops"
DOCK = "dock-scans"                 # the lane: plan revisions, load scans and seals, keyed by outbound trip
ALERTS = "dock-alerts"              # the load check's output, read by every handheld and screen that acts on it
CDC = "cdc-changes"                 # change data capture into the data lake: heavy, latency-tolerant
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {DOCK: 16, ALERTS: 1, CDC: 16}

MISLOAD = "misload"                 # a bag loaded onto a trailer whose current plan does not go to its destination

# Origin gateways for the kit's trailers (Delhivery's FY26 annual report names mega-gateways at Tauru, Lonad
# (Bhiwandi) and Hoskote) and the destinations a trailer can be planned for. Codes and lanes are illustrative.
ORIGINS = ["TAU", "BHW", "HSK"]
DESTS = ["DEL", "BOM", "BLR", "MAA", "HYD", "CCU", "AMD", "PNQ", "JAI", "LKO", "PAT", "GAU", "IDR", "NAG",
         "CJB", "COK", "IXC", "BBI", "IXR", "STV", "VGA", "LUH", "BDQ", "VNS"]


def trip_id(origin: str, n: int) -> str:
    return f"{origin}-T{n:07d}"


def part(key: str, partitions: int) -> int:
    # One partition per trip, the same function in every script, so a trip's plan, loads and seal stay in order.
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
