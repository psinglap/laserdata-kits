"""Shared settings for the payer velocity lane kit: each payment's velocity counts reach the risk check
before the payer's next payment, on Apache Iggy."""
import asyncio
import json
import time
import zlib
from collections import deque

STREAM = "payments-risk"
VELOCITY = "payer-velocity"     # the lane: each completed payment's resulting velocity counts, keyed by payer
CHECKS = "risk-checks"          # a risk check on a payer's next payment, which must already count the last one
EVENTS = "app-events"           # a heavy, latency-tolerant analytics stream on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {VELOCITY: 16, CHECKS: 16, EVENTS: 32}

# Velocity windows the kit keeps per payer. Illustrative: PhonePe does not publish its features;
# its Trust & Safety blog says Guardian "aggregates user events in real time, including ... transaction frequency".
WINDOWS_S = (60, 600)
RULE_COUNT_60S = 5              # an example velocity rule: 5 or more payments by one payer within 60 s


def payer_id(n: int) -> str:
    return f"P{n:09d}"          # illustrative payer IDs


def payee_id(n: int) -> str:
    return f"Q{n:09d}"


def part(payer: str, partitions: int) -> int:
    # One partition per payer, the same function in every script, so a payer's counts stay in order.
    return zlib.crc32(payer.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


class Velocity:
    """Rolling per-payer counts over 60 s and 10 min: payments, amount (paise) and distinct payees.

    Used by payments.py (the source of truth in the kit) and by kafka_bridge.py (on your real payments),
    so the lane always carries resulting values, never a delta.
    """

    def __init__(self):
        self.hist = {}                          # payer -> deque of (t, amount, payee)
        self.seq = {}                           # payer -> sequence of the payer's last payment
        self.last_sweep = time.monotonic()

    def add(self, payer: str, amount: int, payee: str, t: float) -> dict:
        h = self.hist.setdefault(payer, deque())
        h.append((t, amount, payee))
        while h and h[0][0] < t - WINDOWS_S[1]:
            h.popleft()
        recent = [x for x in h if x[0] >= t - WINDOWS_S[0]]
        self.seq[payer] = s = self.seq.get(payer, 0) + 1
        return dict(payer=payer, seq=s, cnt_60s=len(recent), amt_60s=sum(x[1] for x in recent),
                    cnt_10m=len(h), amt_10m=sum(x[1] for x in h), payees_10m=len({x[2] for x in h}))

    def sweep(self, t: float):
        """Forget payers idle for longer than the longest window (run about once a minute)."""
        if t - self.last_sweep < 60:
            return
        self.last_sweep = t
        for p in [p for p, h in self.hist.items() if not h or h[-1][0] < t - WINDOWS_S[1]]:
            del self.hist[p]


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
