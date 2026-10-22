"""Shared settings for the key shutoff lane kit: every API request reaches the enforcement service in
milliseconds, and every key disable reaches every request router before the key's next request, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "api-platform"
REQUESTS = "api-requests"   # the lane: one record per served request, keyed by API key
REPORTS = "key-reports"     # disable requests from outside the request path: leaked keys found by secret scanning
STATUS = "key-status"       # the enforcement service's disables, read by every request router
LOGS = "request-logs"       # a heavy, latency-tolerant stream on the same broker: request and response logs
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {REQUESTS: 16, REPORTS: 1, STATUS: 1, LOGS: 32}

# Cost of one request at API prices, in USD (illustrative; set yours). Abusive traffic on a stolen key runs long
# completions on the largest models, so it costs more per request.
COST_LOW, COST_HIGH = 0.0005, 0.02
ABUSE_LOW, ABUSE_HIGH = 0.01, 0.06


def key(i: int) -> str:
    return f"key_{i:06d}"


def project(k: str) -> str:
    return f"proj_{zlib.crc32(k.encode()) % 4999:04d}"


def home_networks(k: str) -> tuple[str, str]:
    # Where a key's own traffic comes from: its owner's servers sit in one or two networks (illustrative ASNs).
    h = zlib.crc32(k.encode())
    return f"AS{64512 + h % 1000}", f"AS{64512 + (h // 1000) % 1000}"


def part(k: str, partitions: int) -> int:
    # One partition per key, the same function in every script, so a key's requests stay in order.
    return zlib.crc32(k.encode()) % partitions


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
