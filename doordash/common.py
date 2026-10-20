"""Shared settings for the store feedback lane kit: per-store Dasher feedback for dispatch, on Apache Iggy.

The lane carries what Dashers see at each store: a Dasher assigned and heading there, a Dasher
arrived and waiting at the counter, an order picked up (and how long that Dasher waited), each
with the utilization of the store's market at that moment. The dispatch coordinator keeps the
latest feedback for every store and reads it at every dispatch run.
"""
import bisect
import json
import random
import statistics
import time
import zlib

STREAM = "dispatch"
FEEDBACK = "store-feedback"       # the lane: Dasher assigned, arrived, picked up, keyed by store
DECISIONS = "dispatch-decisions"  # each run's decisions, carrying the feedback sequence they were made on
ANALYTICS = "client-analytics"    # a heavy neighbour on the same broker, for the isolation test
FEEDBACK_PARTITIONS, DECISION_PARTITIONS = 16, 8
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once


def store(i: int) -> str:
    return f"ST{i:07d}"


def market(store_id: str, markets: int) -> int:
    return zlib.crc32(b"m" + store_id.encode()) % markets


def part(key: str, partitions: int) -> int:
    # One partition per store, the same function in every script, so a store's feedback stays in order.
    return zlib.crc32(key.encode()) % partitions


def policy(fb) -> tuple[str, float]:
    """Stand-in for the dispatch RL agent: how far to lean from delivery speed toward batching at this store.

    Leans toward batching when Dashers are already waiting at the counter (merchant congestion: an
    extra Dasher sent now would only wait too) and when the store's market is short of free Dashers
    (courier utilization). Returns the mode ("speed" or "batch") and the batching weight, 0 to 1.
    """
    if fb is None:
        return "speed", 0.0
    wait_min = statistics.median(fb["waits"]) if fb["waits"] else 0.0
    w = 0.35 * min(1.0, fb["waiting"] / 3) + 0.25 * min(1.0, wait_min / 8) + 0.4 * max(0.0, (fb["util"] - 70) / 30)
    w = round(min(1.0, w), 2)
    return ("batch" if w >= 0.5 else "speed"), w


class Popularity:
    """Picks stores the way a dinner rush does: a few thousand busy stores take a large share of orders.

    Weights fall off as rank ** -skew. Every script builds the same table, so dashers.py and
    dispatch.py agree on which stores are busy.
    """

    def __init__(self, n: int, skew: float = 0.6):
        self.names = [store(i) for i in range(n)]
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
