"""Shared settings for the boarding-point lane kit: bus GPS packets into boarding-point events and passenger
alerts, keyed by journey (SVC_DOJ), on Apache Iggy, built by LaserData."""
import asyncio
import json
import math
import time
import zlib

STREAM = "yourbus"
GPS, BP_EVENTS, ALERTS, LOGS = "gps-packets", "bp-events", "passenger-alerts", "app-logs"
PARTITIONS = {GPS: 16, BP_EVENTS: 8, ALERTS: 8, LOGS: 8}
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"

FENCE_KM = 0.2       # a bus within 200 m of a boarding point is at it (illustrative; tune per point)
HALT_KM = 0.02       # two packets in a row inside the fence and under 20 m apart: the bus has halted

# Boarding points per source city, in the order a service picks passengers up on its way out of town.
# Names are real places; coordinates are approximate and the routes are illustrative.
CITIES = {
    "BLR": [("Electronic City", 12.8452, 77.6602), ("Silk Board", 12.9177, 77.6238), ("Madiwala", 12.9226, 77.6174),
            ("Shanti Nagar", 12.9566, 77.5980), ("Majestic", 12.9774, 77.5713), ("Hebbal", 13.0358, 77.5970),
            ("Yelahanka", 13.1007, 77.5963)],
    "HYD": [("Miyapur", 17.4968, 78.3614), ("Kukatpally", 17.4849, 78.4138), ("Ameerpet", 17.4375, 78.4483),
            ("Lakdikapul", 17.4033, 78.4626), ("Aramghar", 17.3313, 78.4290), ("Shamshabad", 17.2543, 78.4316)],
    "CHN": [("Koyambedu", 13.0694, 80.1948), ("Vadapalani", 13.0500, 80.2121), ("Guindy", 13.0067, 80.2206),
            ("Tambaram", 12.9249, 80.1000), ("Perungalathur", 12.9050, 80.0877)],
}
DESTS = {"BLR": ["HYD", "CHN", "GOA", "MUM", "CBE"], "HYD": ["BLR", "VJA", "VZG", "CHN"], "CHN": ["BLR", "CBE", "MDU", "HYD"]}


def svc_doj(op: int, src: str, dst: str, dep: str, doj: str) -> str:
    # ServiceNo (operator + source + destination + departure time) and date of journey: the journey's key.
    return f"OP{op:05d}-{src}-{dst}-{dep}_{doj}"


def part(key: str, partitions: int) -> int:
    # One partition per journey, the same function for packets, events and alerts in every script.
    return zlib.crc32(key.encode()) % partitions


def km(a_lat, a_lon, b_lat, b_lon) -> float:
    # Equirectangular distance: accurate to metres at city scale, cheap enough for every packet.
    dx = (b_lon - a_lon) * 111.32 * math.cos(math.radians((a_lat + b_lat) / 2))
    dy = (b_lat - a_lat) * 110.57
    return math.hypot(dx, dy)


class Journey:
    """Boarding-point state for one journey: which point is next, and whether the bus is inside its fence."""
    __slots__ = ("bps", "i", "inside", "halted", "plat", "plon", "seq")

    def __init__(self, bps):
        self.bps = bps                       # [[name, lat, lon, passengers boarding there], ...] in route order
        self.i, self.inside, self.halted = 0, False, False
        self.plat = self.plon = None
        self.seq = 0


def bp_step(j: Journey, lat: float, lon: float) -> list:
    """Apply one GPS packet; return the boarding-point events it causes as [(ENTRY|HALT|EXIT, bp index)].

    ENTRY: the first packet inside the next point's fence. HALT: the first packet inside that is under 20 m from
    the one before. EXIT: the first packet outside again; the next point becomes the target.
    """
    out = []
    if j.i < len(j.bps):
        _, blat, blon, _ = j.bps[j.i]
        inside = km(lat, lon, blat, blon) <= FENCE_KM
        if not j.inside:
            if inside:
                j.inside = True
                out.append(("ENTRY", j.i))
        elif inside:
            if not j.halted and j.plat is not None and km(lat, lon, j.plat, j.plon) <= HALT_KM:
                j.halted = True
                out.append(("HALT", j.i))
        else:
            out.append(("EXIT", j.i))
            j.i, j.inside, j.halted = j.i + 1, False, False
    j.plat, j.plon = lat, lon
    return out


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


async def read_partitions(client, topic, pids, handler, name, batch=2000, poll_ms=1.0):
    """Read the given partitions of a topic directly, each in turn, from the start of the log, then from this
    reader's own position. No consumer group: a group reader visits one partition per poll, so a packet would
    wait its turn behind the others. Nothing is stored on the broker; a restart rebuilds from the retained log.
    A full batch means more is waiting, so the next round starts at once."""
    from apache_iggy import Consumer, PollingStrategy
    who = {p: Consumer.Single(f"{name}-p{p}") for p in pids}
    nxt = {}
    while True:
        more = False
        for p in pids:
            ms = await client.poll_messages(STREAM, topic, consumer=who[p], partition_id=p, count=batch,
                                            auto_commit=False,
                                            polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                            else PollingStrategy.First())
            if ms:
                nxt[p] = ms[-1].offset() + 1
                for m in ms:
                    handler(m)
                more = more or len(ms) == batch
        if not more:
            await asyncio.sleep(poll_ms / 1000)


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
