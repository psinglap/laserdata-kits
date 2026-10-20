"""Shared settings for the driver ping lane kit: driver-partner location for live trip tracking, on Apache Iggy."""
import json
import math
import time
import zlib

STREAM = "intracity"
PINGS, TRIPS, MILESTONES = "driver-pings", "trip-events", "trip-milestones"   # the lane, order lifecycle, arrivals
LOGS = "app-logs"                          # a busy neighbour, for the log-burst test
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090"
CITY = (12.86, 77.50, 13.08, 77.72)        # a box over Bengaluru: lat_min, lon_min, lat_max, lon_max
PICKUP_AREAS = {                           # where goods are loaded: markets and industrial areas (illustrative)
    "KR Market": (12.9634, 77.5775),
    "Chickpet": (12.9700, 77.5780),
    "Yeshwanthpur APMC": (13.0265, 77.5410),
    "Peenya Industrial Area": (13.0285, 77.5197),
    "Electronic City": (12.8452, 77.6602),
}
# Vehicle classes: share of drivers online and road speed in km/h (illustrative). Two-wheelers are
# about 60% of Porter's orders (The Ken, Sep 2025); trucks are slower and load for longer.
VEHICLES = {
    "2W":         (0.60, (18, 30), (60, 300)),     # share, speed range, loading/unloading seconds
    "3W":         (0.12, (14, 24), (180, 600)),
    "MINI_TRUCK": (0.18, (12, 22), (300, 900)),
    "PICKUP_8FT": (0.10, (10, 20), (300, 1200)),
}
FENCE_KM = 0.15                            # a driver within 150 m of the pickup or drop has reached it


def driver(i: int) -> str:
    return f"DP{i:07d}"


def part(key: str, partitions: int) -> int:
    # One partition per driver, the same function for pings and trip events in every script.
    return zlib.crc32(key.encode()) % partitions


def km(a_lat, a_lon, b_lat, b_lon) -> float:
    # Equirectangular distance: accurate to metres at city scale, cheap enough for every ping.
    dx = (b_lon - a_lon) * 111.32 * math.cos(math.radians((a_lat + b_lat) / 2))
    dy = (b_lat - a_lat) * 110.57
    return math.hypot(dx, dy)


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


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
