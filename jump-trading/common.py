"""Shared settings for the link health alert lane kit: every telemetry sample from a network link (RSSI, SNR,
BER) reaches the baseline and alert rules in milliseconds, in order per link, and every degraded or recovered
alert reaches the network console, on-call and the status board while the link is still degraded, on
Apache Iggy."""
import asyncio
import gc
import json
import random
import time
import zlib

STREAM = "network-telemetry"
SAMPLES = "link-samples"            # the lane: one sample per link every few ms, keyed by link
ALERTS = "link-alerts"              # the rules' output, read by every consumer (console, on-call, status board)
LOGS = "host-logs"                  # log files shipped from hosts: bulky, bursty, latency-tolerant
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {SAMPLES: 16, ALERTS: 1, LOGS: 16}

# Alert kinds (illustrative; your rule names go here).
DEGRADED = "link_degraded"          # BER above the limit, or SNR well below the link's own baseline
RECOVERED = "link_recovered"        # back inside its baseline for CLEAR_SAMPLES samples in a row

BER_MAX = 1e-6                      # bit error rate above which a link counts as degraded (illustrative)
SNR_DROP_DB = 6.0                   # dB below the link's learned SNR baseline (illustrative)
CLEAR_SAMPLES = 5                   # clean samples in a row before RECOVERED
BASELINE_ALPHA = 0.02               # weight of each clean sample in the SNR baseline (an EWMA)


def link(i: int) -> str:
    return f"mw-{i:04d}"


def part(key: str, partitions: int) -> int:
    # One partition per link, the same function in every script, so a link's samples stay in order.
    return zlib.crc32(key.encode()) % partitions


def nominal(l: str):
    """The link's clear-sky SNR (dB) and RSSI (dBm): illustrative, fixed per link, the same in every script."""
    r = random.Random(l)
    return round(r.uniform(32, 42), 1), round(r.uniform(-48, -38), 1)


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
