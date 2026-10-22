"""Shared settings for the Soundbox payment alert lane kit: every confirmed payment for a device merchant reaches
the announcement service in milliseconds, in order per Soundbox, and is announced once, while payments that were
confirmed long after the customer paid (a bank recovering from an outage) go to the merchant's passbook on their
own topic instead of the speaker, on Apache Iggy."""
import asyncio
import gc
import json
import time
import zlib

STREAM = "soundbox"
LANE = "payment-confirmations"      # the lane: payment status for device merchants, keyed by Soundbox
LATE = "late-confirmations"         # payments confirmed long after the customer paid: for the passbook, not the speaker
ANNOUNCE = "soundbox-announcements"  # the announcement service's output, read by the device push and the app
APP_EVENTS = "app-events"           # app and behavioural events for analytics: big, steady (the neighbour)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {LANE: 16, LATE: 8, ANNOUNCE: 1, APP_EVENTS: 8}

# Payment status kinds (illustrative names; yours come from the payment status catalogue). Every event carries
# the device's resulting state after it (today's collections in paise and today's payment count, which the
# screen Soundboxes show), never a delta, so a repeat can't change a total.
UPI = "upi_success"
CARD = "card_success"
PENDING = "pending"                 # status updates that are not money in: no announcement
FAILED = "failed"
QUIET = (PENDING, FAILED)
ANNOUNCE_KINDS = (UPI, CARD)

# How long after the customer paid a confirmation may still be announced. Later than that, the customer has
# usually left and someone else may be at the counter, so the payment is credited to today's total and shown in
# the passbook, and the speaker stays quiet. 60 s is illustrative; set it to yours.
FRESH_S = 60

# The 11 languages Paytm's Soundbox announces in (an illustrative device setting, used in the announcement).
LANGS = ("hi", "en", "bn", "te", "mr", "ta", "gu", "kn", "ml", "pa", "or")


def device(i: int) -> str:
    return f"sbx-{i:08d}"           # an illustrative Soundbox ID, not a real device or merchant


def part(key: str, partitions: int) -> int:
    # One partition per Soundbox, the same function in every script, so a device's payments stay in order.
    return zlib.crc32(key.encode()) % partitions


def route(kind: str, vt_ns: int, now_ns: int) -> str:
    """Where the confirmation step puts a status event: on the live lane if the customer paid within FRESH_S,
    otherwise on the late topic. The time the customer paid decides, not arrival order, so a burst of payments
    confirmed after a bank outage never queues ahead of the payment being made at the counter now."""
    if now_ns - vt_ns <= FRESH_S * 1e9:
        return LANE
    return LATE


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


def rupees(paise: int) -> str:
    return f"₹{paise / 100:,.2f}"


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


class Stalls:
    """Longest garbage-collection pause and longest event-loop stall since the last read, in ms. Printed next to
    the latency, so a slow second can be traced to this process (or ruled out) rather than guessed at."""

    def __init__(self):
        self.gc = self.loop = 0.0
        self._t = 0.0
        gc.callbacks.append(self._cb)

    def _cb(self, phase, info):
        if phase == "start":
            self._t = time.perf_counter()
        else:
            self.gc = max(self.gc, (time.perf_counter() - self._t) * 1000)

    async def watch(self):
        while True:
            t = time.perf_counter()
            await asyncio.sleep(0.005)
            self.loop = max(self.loop, (time.perf_counter() - t - 0.005) * 1000)

    def read(self):
        out = f"stall gc {self.gc:.0f}ms loop {self.loop:.0f}ms"
        self.gc = self.loop = 0.0
        return out
