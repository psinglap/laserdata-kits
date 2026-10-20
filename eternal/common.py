"""Shared settings for the kitchen state lane kit: per-restaurant kitchen state for food preparation time, on Apache Iggy."""
import bisect
import json
import random
import time
import zlib

STREAM = "food-delivery"
KITCHEN = "kitchen-state"         # the lane: order accepted, food ready, picked up, keyed by restaurant
ORDERS = "orders"                 # checkouts, each carrying the FPT prediction made for it
REPLAY = "benchmark-replay"       # a heavy neighbour on the same broker, for the isolation test
KITCHEN_PARTITIONS, ORDER_PARTITIONS = 16, 8
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090"


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
