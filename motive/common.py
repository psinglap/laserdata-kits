"""Shared settings for the cargo alert lane kit: every Door and Environmental Sensor event from a trailer's
Asset Gateway reaches the alert rules in milliseconds, in order per asset, and every alert reaches the
driver and the back office while the door is still open, on Apache Iggy."""
import asyncio
import functools
import json
import math
import random
import time
import zlib

STREAM = "equipment-monitoring"
READINGS = "cargo-sensor-events"    # the lane: door changes and one-minute readings, keyed by asset
ALERTS = "cargo-alerts"             # the alert rules' output, read by every notifier (driver app, dashboard, webhooks)
BACKFILL = "gateway-backfill"       # readings buffered in dead zones, uploaded on reconnect: heavy, latency-tolerant
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {READINGS: 16, ALERTS: 1, BACKFILL: 16}

# Alert kinds (illustrative; your rule names go here).
DOOR_OUTSIDE = "door_open_outside_geofence"   # a cargo door opened outside every approved geofence
TEMP_HIGH = "temperature_out_of_range"        # cargo temperature above the asset's range

GEOFENCE_M = 300                    # radius of an approved geofence (yard, customer site), metres
TEMP_BAND = 3.0                     # degrees C above the set point before a temperature alert (illustrative)
DOOR_CAUSE_S = 900                  # a door change in the last 15 min is named as the likely cause of an excursion


def asset(i: int) -> str:
    return f"trl-{i:07d}"


def part(key: str, partitions: int) -> int:
    # One partition per asset, the same function in every script, so an asset's events stay in order.
    return zlib.crc32(key.encode()) % partitions


@functools.lru_cache(maxsize=None)
def sites(a: str):
    """The asset's approved geofences: its yard and two customer sites (illustrative; yours come from the
    geofences fleets set in the dashboard). The same in every script, so the kit needs no lookup service."""
    r = random.Random(a)
    lat0, lon0 = r.uniform(30, 45), r.uniform(-120, -80)
    return [(lat0 + r.uniform(-0.5, 0.5), lon0 + r.uniform(-0.5, 0.5)) for _ in range(3)]


def set_point(a: str) -> float:
    return -18.0 if zlib.crc32(a.encode()) % 3 == 0 else 3.0      # frozen or chilled loads (illustrative)


def metres(a, b) -> float:
    dlat = math.radians(b[0] - a[0])
    dlon = math.radians(b[1] - a[1]) * math.cos(math.radians((a[0] + b[0]) / 2))
    return 6_371_000 * math.hypot(dlat, dlon)


def inside_approved(a: str, pos) -> bool:
    return any(metres(pos, s) <= GEOFENCE_M for s in sites(a))


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
