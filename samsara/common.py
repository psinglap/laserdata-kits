"""Shared settings for the Data Connector stream lane kit: one Apache Iggy topic per stream subscription.

Samsara's Data Connectors push fleet telemetry, events and entity changes to each customer's own
message broker (self-run Kafka, MSK, Event Hubs, Pub/Sub). Each stream subscription sends one
entity (GPS, engine states, fuel levels, temperatures, events) to one customer topic. Here every
subscription gets its own topic in the data-streams stream, keyed by vehicle ID. The datapoint
writer (datapoints.py) appends each updated datapoint to the subscription of its org and entity;
each connector worker (connector.py) reads its subscriptions at their destination's pace from their
own offsets, so a customer's broker that is down or throttling only ever delays its own stream,
and the log keeps the 6 hours Samsara documents for recovery.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "data-streams"
RAW_STREAM = "ingest"                                     # raw sensor uploads next door (ingest_flood.py)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
RECOVERY = timedelta(hours=6)                             # Samsara's documented data recovery window
RETENTION = IggyExpiry.ExpireDuration(RECOVERY)

# Entities a stream subscription can carry, named after the Kafka Connector reference, and how often a
# vehicle produces one per GPS reading (GPS: every 5 s while the vehicle is on, every 5 min when off).
ENTITIES = {"gps": 1.0, "events": 0.03, "engine-states": 0.05, "fuel-levels": 0.10, "temperatures": 0.20}
EVENT_TYPES = ("geofenceEntry", "geofenceExit", "routeStopArrival", "routeStopDeparture",
               "severeSpeedingStarted", "severeSpeedingEnded", "engineFaultOn", "dvirSubmitted")


def org_name(i: int) -> str:
    return f"org-{i + 1:04d}"


def entities_for(i: int) -> list:
    """Which entities org i streams: every org streams GPS; every 2nd events, 3rd engine states, 4th fuel, 5th temps."""
    return ["gps"] + [e for e, k in (("events", 2), ("engine-states", 3), ("fuel-levels", 4), ("temperatures", 5))
                      if i % k == 0]


def weights(n: int) -> list:
    """Fleet sizes: a few very large fleets and a long tail (Zipf-like), summing to 1."""
    w = [1 / (i + 1) ** 0.9 for i in range(n)]
    s = sum(w)
    return [x / s for x in w]


def partitions_for(i: int, n: int) -> int:
    # The largest 5% of fleets get 4 partitions per subscription; everyone else 1. Same function in every script.
    return 4 if i < max(1, n // 20) else 1


def subscriptions(n: int) -> dict:
    """Every stream subscription of the kit's n orgs: topic name -> (org index, entity, partitions)."""
    return {f"{org_name(i)}-{e}": (i, e, partitions_for(i, n)) for i in range(n) for e in entities_for(i)}


def vehicle_id(org: int, k: int) -> str:
    return f"2814{org:04d}{k:07d}"                        # illustrative 15-digit vehicle IDs


def shard_of(topic: str, shards: int) -> int:
    return zlib.crc32(topic.encode()) % shards


def part(vehicle: str, partitions: int) -> int:
    # Keyed by vehicle ID: one vehicle's datapoints stay in order on one partition.
    return zlib.crc32(vehicle.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topic(c: IggyClient, stream: str, topic: str, partitions: int, expiry=RETENTION):
    """Create the stream and topic if they are missing, so any script can start first."""
    if await c.get_stream(stream) is None:
        try:
            await c.create_stream(stream)
        except RuntimeError:
            pass                                    # created by another process in the meantime
    if await c.get_topic(stream, topic) is None:
        try:
            await c.create_topic(stream, topic, partitions, message_expiry=expiry)
        except RuntimeError:
            pass
    while not await c.get_topic(stream, topic):
        await asyncio.sleep(0.2)


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
