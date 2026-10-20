"""Shared settings for the spend-limit lane kit: every billable request's usage reaches the spend-limit check
in milliseconds, and every stop reaches every API server, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "metering"
USAGE = "usage"                 # the lane: each API server's running paid usage per user, keyed by user
DECISIONS = "limit-decisions"   # the limit service's decisions back to every API server: alert or stop
TAB = "tab-events"              # a heavy, latency-tolerant stream on the same broker: Tab accept/reject events
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {USAGE: 16, DECISIONS: 1, TAB: 32}

# Cost of one billable request (an agent model call at on-demand rates): $0.01-$0.20 (illustrative; set yours).
COST_LOW, COST_HIGH = 0.01, 0.20
AVG_COST = (COST_LOW + COST_HIGH) / 2


def user(i: int) -> str:
    return f"usr_{i:06d}"


def team(u: str) -> str:
    return f"team_{zlib.crc32(u.encode()) % 997:03d}"


def part(key: str, partitions: int) -> int:
    # One partition per user, the same function in every script, so a user's usage stays in order.
    return zlib.crc32(key.encode()) % partitions


def period(period_s: float, now: float | None = None) -> int:
    # The kit compresses a billing period into --period-s seconds; every script derives it from the clock.
    return int((time.time() if now is None else now) // period_s)


def _frac(key: str) -> float:
    return (zlib.crc32(key.encode()) % 1000) / 1000


def limit(u: str, requests_per_s: float, users: int, period_s: float) -> tuple[float, str]:
    """A user's spend limit for one period in USD, and where it came from (illustrative values).

    Like Cursor's spend limits, a user can have a team default, a group override and a member override,
    and the highest applicable limit wins. Even share = what an average user spends in a period.
    About a third have a raised member override and never reach it, so traffic never stops; the rest
    reach their limit somewhere in the period."""
    even = requests_per_s * AVG_COST * period_s / users
    h = zlib.crc32(u.encode())
    found = {"team": even * (0.3 + 0.5 * _frac(team(u)))}
    if h % 4 == 1:
        found["group"] = even * (0.5 + 0.5 * _frac(u + "/group"))
    if h % 3 == 0:
        found["member"] = even * 6.0
    src = max(found, key=found.get)
    return round(found[src], 4), src


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
