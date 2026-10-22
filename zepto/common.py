"""Shared settings for the hub stock lane kit: per-hub inventory and pricing on Apache Iggy."""
import json
import time

STREAM = "search-domain"
INVENTORY, PRICING, TAGS = "inventory", "pricing", "tags"   # P0, P0, P1
ORDERS, STATUS = "orders", "order-status"
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"


def key(hub: int, sku: int) -> str:
    return f"H{hub:03d}:S{sku:04d}"


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


class Window:
    def __init__(self):
        self.samples, self.start = [], time.monotonic()

    def add(self, v):
        self.samples.append(v)

    def flush(self):
        s = sorted(self.samples) or [0.0]
        pick = lambda q: s[min(len(s) - 1, int(q * len(s)))]
        out = {"n": len(self.samples), "p50": pick(0.5), "p99": pick(0.99), "p999": pick(0.999), "max": s[-1]}
        self.samples, self.start = [], time.monotonic()
        return out
