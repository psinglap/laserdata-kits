"""Shared settings for the kitchen state lane kit: per-restaurant kitchen state for food preparation time, on Apache Iggy."""
import asyncio
import bisect
import json
import random
import time
import zlib

from apache_iggy import Consumer, PollingStrategy

STREAM = "food-delivery"
KITCHEN = "kitchen-state"         # the lane: order accepted, food ready, picked up, keyed by restaurant
ORDERS = "orders"                 # checkouts, each carrying the FPT prediction made for it
REPLAY = "benchmark-replay"       # a heavy neighbour on the same broker, for the isolation test
KITCHEN_PARTITIONS, ORDER_PARTITIONS = 16, 8
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"


def restaurant(i: int) -> str:
    return f"RES{i:06d}"


def part(key: str, partitions: int) -> int:
    # One partition per restaurant, the same function in every script, so a kitchen's events stay in order.
    return zlib.crc32(key.encode()) % partitions


class Popularity:
    """Picks restaurants the way a dinner rush does: a few hundred busy kitchens take a large share.

    Weights fall off as rank ** -skew. Every script builds the same table, so kitchens.py and fpt.py
    agree on which restaurants are busy.
    """

    def __init__(self, n: int, skew: float = 0.7):
        self.names = [restaurant(i) for i in range(n)]
        total, self.cum = 0.0, []
        for rank in range(1, n + 1):
            total += rank ** -skew
            self.cum.append(total)
        self.total = total

    def pick(self, rnd=random) -> str:
        return self.names[min(len(self.names) - 1, bisect.bisect(self.cum, rnd.random() * self.total))]


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


class GcPauses:
    """Longest Python garbage-collection pause since the last read, in ms (for --why)."""

    def __init__(self):
        import gc
        self.longest, self._t = 0.0, 0.0
        gc.callbacks.append(self._cb)

    def _cb(self, phase, info):
        if phase == "start":
            self._t = time.perf_counter()
        else:
            self.longest = max(self.longest, (time.perf_counter() - self._t) * 1000)

    def take(self) -> float:
        v, self.longest = self.longest, 0.0
        return v


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
