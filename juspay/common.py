"""Shared settings for the release guard lane kit: every payment outcome, tagged with the release that
served it, reaches the A/B comparison in milliseconds, on Apache Iggy."""
import asyncio
import json
import math
import time
import zlib

STREAM = "release-guard"
OUTCOMES = "payment-outcomes"   # the lane: each payment attempt's final status, with the release that served it
VERDICTS = "release-verdicts"   # the comparison's decisions back to the rollout: SCALE, HOLD or ABORT
API_LOGS = "api-logs"           # a heavy, latency-tolerant API-log stream on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {OUTCOMES: 16, VERDICTS: 1, API_LOGS: 32}

# Juspay's engineering post: Autopilot staggers a new deployment's traffic "from 1% onwards", and the
# A/B testing framework "automatically decides to scale up the traffic or abort the deployment".
# The ladder below is illustrative; set it to yours.
LADDER = (1, 5, 10, 25, 50, 100)
SUCCESS = "charged"             # Hyperswitch attempt statuses the comparison counts as a success
SUCCESSES = {SUCCESS, "authorized", "partial_charged"}


def part(payment_id: str, partitions: int) -> int:
    # One partition per payment, the same function in every script, so a payment's updates stay in order.
    return zlib.crc32(payment_id.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


def z_score(ok_c: int, n_c: int, ok_s: int, n_s: int) -> float:
    """Two-proportion z-test, canary minus stable. Negative means the canary succeeds less often."""
    if n_c == 0 or n_s == 0:
        return 0.0
    p = (ok_c + ok_s) / (n_c + n_s)
    se = math.sqrt(max(p * (1 - p), 1e-9) * (1 / n_c + 1 / n_s))
    return (ok_c / n_c - ok_s / n_s) / se


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
