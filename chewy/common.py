"""Shared settings for the fulfillment center edge lane kit: every carton event on the floor (picked, packed,
labeled, sorted to a dock door, loaded, ship-confirmed) lands in an event log inside the building in
milliseconds, keeps flowing when the building's link to AWS is down, and reaches the cloud afterwards in order,
each event once. On Apache Iggy, built by LaserData.

Two brokers in production: one small broker in the fulfillment center (the site log) and one in AWS (the cloud
log). The kit runs both on one broker by default; pass --cloud-url to point the uplink and the cloud ledger at a
second one.
"""
import asyncio
import json
import zlib

from apache_iggy import IggyClient

SITE = "AVP2"                        # a site code as Chewy's job listings name a fulfillment center (Jessup, PA)
FC = "fc"                            # stream on the building's broker
EVENTS = "carton-events"             # the lane: one message per carton event, keyed by carton
WCS = "wcs-telemetry"                # the neighbour: conveyor, sorter and scanner telemetry, on its own topic
CLOUD = "fulfillment"                # stream on the AWS broker
CLOUD_EVENTS = "fc-carton-events"    # every site's carton events, keyed by site:carton
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {EVENTS: 8, WCS: 4, CLOUD_EVENTS: 8}
STREAM_OF = {EVENTS: FC, WCS: FC, CLOUD_EVENTS: CLOUD}

# A carton's life on the floor, in order. Each event carries the carton's resulting step, never a delta.
STEPS = ["PICKED", "PACKED", "LABELED", "SORTED", "LOADED", "SHIP_CONFIRMED"]
DONE = len(STEPS)                    # seq of the last event: SHIP_CONFIRMED is the carton's 6th event


def carton_id(site: str, n: int) -> str:
    return f"{site}-C{n:09d}"


def part(key: str, partitions: int) -> int:
    # One partition per carton, the same function in every script, so a carton's events stay in order.
    return zlib.crc32(key.encode()) % partitions


def msg(**d) -> bytes:
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def ensure_topics(client, *topics):
    """Create the streams and topics if they are missing, so any script can start first."""
    for t in topics:
        s = STREAM_OF[t]
        if await client.get_stream(s) is None:
            try:
                await client.create_stream(s)
            except RuntimeError:
                pass                                # created by another script in the meantime
        if await client.get_topic(s, t) is None:
            try:
                await client.create_topic(s, t, PARTITIONS[t])
            except RuntimeError:
                pass
    while not all([await client.get_topic(STREAM_OF[t], t) for t in topics]):
        await asyncio.sleep(0.5)


async def connect(url):
    """One connection per producer and per reader: sharing one slowed readers down."""
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


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
