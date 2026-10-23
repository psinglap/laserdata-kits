"""Shared settings for the position share lane kit: every truck position from a connected carrier fleet and every
change to that vehicle's shipments, keyed by vehicle, reach the share gate in order, so a position goes to a
customer only while that customer's shipment is open on that truck, on Apache Iggy."""
import asyncio
import gc
import json
import math
import time
import zlib

STREAM = "carrier-visibility"
LANE = "fleet-positions"            # the lane: positions and shipment changes (assign, stop), keyed by vehicle
UPDATES = "shipment-updates"        # the gate's output: shared positions, tracking started and completed
REPLAY = "cv-replay"                # a 72-hour replay of other topics: heavy, latency-tolerant
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {LANE: 16, UPDATES: 1, REPLAY: 16}

# The data-sharing rule, as project44's carrier help centre describes it ("How Data Is Shared"): sharing starts
# when the right vehicle enters the outer geofence around the loading place (usually a 50 km radius) inside the
# shipment's time window (usually from 90 minutes before the planned pick-up); it stops as soon as the vehicle
# drives out of the delivery geofence, or after it has stayed inside it (usually 3 hours). Positions from a vehicle
# with no open shipment are not shared. The delivery-geofence radius is not published; 1 km is a stand-in.
OUTER_KM = 50.0
DROP_KM = 1.0
DWELL_S = 3 * 3600

PENDING, TRACKING, DONE = 0, 1, 2

# Shipping and receiving locations for the kit's trucks, around Chicago (illustrative).
SITES = {
    "CHI": (41.85, -87.65), "JOL": (41.53, -88.08), "AUR": (41.76, -88.32), "ELG": (42.04, -88.28),
    "RFD": (42.27, -89.09), "MKE": (43.04, -87.91), "KEN": (42.58, -87.82), "MSN": (43.07, -89.40),
    "GYY": (41.59, -87.35), "SBN": (41.68, -86.25), "LAF": (40.42, -86.88), "IND": (39.77, -86.16),
    "PIA": (40.69, -89.59), "BMI": (40.48, -88.99), "CMI": (40.12, -88.24), "DPA": (41.91, -88.25),
}


def vehicle_id(n: int) -> str:
    return f"TRK-{n:07d}"


def part(key: str, partitions: int) -> int:
    # One partition per vehicle, the same function in every script, so a truck's shipment changes and its
    # positions stay in the order they happened.
    return zlib.crc32(key.encode()) % partitions


def km(a, b) -> float:
    """Distance between two (lat, lon) points in km (equirectangular; fine at geofence scale)."""
    x = math.radians(b[1] - a[1]) * math.cos(math.radians((a[0] + b[0]) / 2))
    y = math.radians(b[0] - a[0])
    return 6371.0 * math.hypot(x, y)


def step(sh: list, lat: float, lon: float, ns: int, dwell_s: float = DWELL_S):
    """Apply one position to one shipment on that truck. sh = [state, pickup, drop, window start ns, arrived ns].
    Returns (event, share): event is "start", "end" or None; share is whether this position goes to the customer.
    The same function runs in the gate and in fleet.py's truth, so any difference comes from the lane alone."""
    p = (lat, lon)
    if sh[0] == PENDING:
        if ns >= sh[3] and km(p, sh[1]) <= OUTER_KM:
            sh[0] = TRACKING
            return "start", True
        return None, False
    if sh[0] == TRACKING:
        if km(p, sh[2]) <= DROP_KM:
            if sh[4] is None:
                sh[4] = ns                  # arrived at the delivery location
            elif ns - sh[4] > dwell_s * 1e9:
                sh[0] = DONE                # stayed inside the delivery geofence too long: completed
                return "end", False
            return None, True
        if sh[4] is not None:
            sh[0] = DONE                    # drove out of the delivery geofence after arriving: completed
            return "end", False
        return None, True
    return None, False


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
