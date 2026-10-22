"""Shared settings for the captain location lane kit: on-duty captain pings for ride matching, on Apache Iggy."""
import json
import math
import time
import zlib

STREAM = "captain-lane"
PINGS, RIDES, OFFERS, PICKUP = "captain-pings", "ride-requests", "ride-offers", "pickup-track"
COMMS = "rce-events"                        # a busy neighbour, for the campaign-burst test
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"
CITY = (17.30, 78.30, 17.55, 78.60)        # a box over Hyderabad: lat_min, lon_min, lat_max, lon_max
HOTSPOTS = {                                # where office and college peak requests start (illustrative)
    "HITEC City": (17.4435, 78.3772),
    "Gachibowli": (17.4401, 78.3489),
    "Ameerpet": (17.4375, 78.4483),
    "Madhapur": (17.4483, 78.3915),
    "Secunderabad": (17.4399, 78.4983),
}
CELL_DEG = 0.01                             # ~1.1 km square cells; stand-in for the hex cells matching uses


def captain(i: int) -> str:
    return f"CAP{i:07d}"


def part(key: str, partitions: int) -> int:
    # One partition per captain, the same function for pings everywhere.
    return zlib.crc32(key.encode()) % partitions


def cell(lat: float, lon: float) -> tuple:
    return int(lat / CELL_DEG), int(lon / CELL_DEG)


def km(a_lat, a_lon, b_lat, b_lon) -> float:
    # Haversine distance in km.
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    dp, dl = p2 - p1, math.radians(b_lon - a_lon)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 12742.0 * math.asin(math.sqrt(h))


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
