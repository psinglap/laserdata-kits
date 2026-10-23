"""Shared settings for the delivery partner ping lane kit: partner location and last-mile ETA on Apache Iggy."""
import asyncio
import json
import math
import time
import zlib

from apache_iggy import Consumer, PollingStrategy

STREAM = "last-mile"
PINGS, LEGS, ETA = "partner-pings", "order-legs", "eta"   # the lane, trip state, ETAs for the tracking screen
STORM = "order-events"                                     # a busy neighbour, for the retry-storm test
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"
CITY = (12.86, 77.52, 13.06, 77.72)       # a box over Bengaluru: lat_min, lon_min, lat_max, lon_max
SILK_BOARD = (12.9174, 77.6229)           # traffic slows inside 1.5 km of it (illustrative)


def partner(i: int) -> str:
    return f"DE{i:06d}"


def part(key: str, partitions: int) -> int:
    # One partition per partner, the same function for pings and trip legs in every script.
    return zlib.crc32(key.encode()) % partitions


def km(a_lat, a_lon, b_lat, b_lon) -> float:
    # Haversine distance, as Swiggy's last-mile ETA post describes for its ping features.
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


async def read_partitions(client, topic, partitions, name, handle, *, replay=False, commit_s=None, batch=5000, poll_ms=1.0):
    # Direct reads on explicit partitions, not a consumer group: a group visits one partition per poll,
    # which added 10-25 ms of queueing on CI runners. One connection polls each partition in turn.
    # replay=True rebuilds from the first message; otherwise resume from the stored offset, stored on the
    # broker at most once every commit_s seconds (never on every poll).
    consumers = {p: Consumer.Single(f"{name}-p{p}") for p in partitions}
    nxt, committed = {}, {}
    while True:
        more = False
        for p in partitions:
            start = PollingStrategy.Offset(nxt[p]) if p in nxt else PollingStrategy.First() if replay else PollingStrategy.Next()
            commit = commit_s is not None and time.monotonic() - committed.get(p, 0) >= commit_s
            ms = await client.poll_messages(STREAM, topic, consumer=consumers[p], partition_id=p, count=batch,
                                            auto_commit=commit, polling_strategy=start)
            if ms:
                if commit:
                    committed[p] = time.monotonic()
                nxt[p] = ms[-1].offset() + 1
                for m in ms:
                    await handle(m)
                more = more or len(ms) == batch
        if not more:
            await asyncio.sleep(poll_ms / 1000)
