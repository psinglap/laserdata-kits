"""Shared settings for the Afterpay payment token lane kit: every payment token change from the payments platform's
scheduled jobs reaches every subscribing service's token cache in milliseconds, signed and free of card numbers,
on Apache Iggy."""
import asyncio
import hashlib
import hmac
import json
import os
import re
import time
import zlib

from apache_iggy import IggyClient

STREAM = "payments-platform"
TOKENS = "payment-token-changes"   # the lane: one record per token change, keyed by customer, resulting token
CHARGES = "instalment-charges"     # each instalment charge a subscriber sends, with the token version it used
FEED = "warehouse-feed"            # a heavy, latency-tolerant stream on the same broker (payment events for the warehouse)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {TOKENS: 16, CHARGES: 8, FEED: 16}

# The signing key. In production this is a KMS key both sides may use; here it comes from the environment,
# with a demo default so the kit runs out of the box.
KEY = os.environ.get("TOKEN_LANE_HMAC_KEY", "demo-only-not-a-secret").encode()


def customer(i: int) -> str:
    return f"C{i:08d}"


def part_of(k: str, partitions: int = PARTITIONS[TOKENS]) -> int:
    # The one partition function used everywhere: a customer's token changes always land in one partition, in order.
    return zlib.crc32(k.encode()) % partitions


def luhn_ok(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = ord(ch) - 48
        if i % 2:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


_RUN = re.compile(r"(?<!\d)\d{13,19}(?!\d)")


def card_numbers(text: str) -> list[str]:
    """Digit runs that look like a card number: 13 to 19 digits that pass the Luhn check."""
    return [m for m in _RUN.findall(text) if luhn_ok(m)]


def mask_card_numbers(text: str) -> tuple[str, int]:
    """Replace every card-number-shaped run with its last four digits, before anything is signed or sent."""
    n = 0

    def sub(m):
        nonlocal n
        if not luhn_ok(m.group(0)):
            return m.group(0)
        n += 1
        return "*" * (len(m.group(0)) - 4) + m.group(0)[-4:]

    return _RUN.sub(sub, text), n


def strings_with_card_numbers(d: dict) -> int:
    """How many text fields of a record hold a card number. Only text is scanned: numbers such as the
    nanosecond timestamp are long digit runs too, and are not card data."""
    return sum(1 for v in d.values() if isinstance(v, str) and card_numbers(v))


def mask_record(d: dict) -> int:
    """Mask card numbers in every text field of a record, in place; returns how many were masked."""
    total = 0
    for f, v in d.items():
        if isinstance(v, str):
            d[f], n = mask_card_numbers(v)
            total += n
    return total


def sign(body: bytes) -> bytes:
    return body + b"|" + hmac.new(KEY, body, hashlib.sha256).hexdigest().encode()


def verify(payload: bytes) -> bytes | None:
    """The signed body, or None if the signature does not match."""
    body, _, sig = payload.rpartition(b"|")
    good = hmac.new(KEY, body, hashlib.sha256).hexdigest().encode()
    return body if hmac.compare_digest(sig, good) else None


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


async def ensure_topics(c: IggyClient, *topics):
    """Create the stream and topics if they are missing, so any script can start first."""
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    for t in topics:
        if await c.get_topic(STREAM, t) is None:
            try:
                await c.create_topic(STREAM, t, PARTITIONS[t])
            except RuntimeError:
                pass
    while not all([await c.get_topic(STREAM, t) for t in topics]):
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
