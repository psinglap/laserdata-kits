"""Shared settings for the Forge usage alerts lane: every Forge function's usage reaches its app's allowance
check in milliseconds, counted once even when a record is sent twice, and every 50/75/90/100% crossing
reaches the alert service, on Apache Iggy."""
import asyncio
import bisect
import calendar
import json
import math
import time
import zlib

STREAM = "forge-usage"
USAGE = "usage"                 # the lane: each runtime host's running usage per app this cycle, keyed by app
ALERTS = "usage-alerts"         # threshold crossings from the allowance tracker to the alert service
ANALYTICS = "product-analytics" # a heavy, latency-tolerant stream on the same broker: clicks and impressions
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {USAGE: 16, ALERTS: 1, ANALYTICS: 32}

# Forge Functions duration, from Atlassian's Forge platform pricing page (read Oct 2026): 200,000 GB-seconds
# free per app per month, then USD 0.000025 per GB-second. Memory per invocation is illustrative; set yours.
PRICE_PER_GBS = 0.000025
MONTHLY_ALLOWANCE_GBS = 200_000
MEM_GB = 0.5
# Invocation duration in seconds: log-normal, median 0.2 s, mean about 0.3 s (illustrative).
DUR_MU, DUR_SIGMA = -1.6, 0.9
MEAN_GBS = MEM_GB * math.exp(DUR_MU + DUR_SIGMA ** 2 / 2)
LEVELS = (50, 75, 90, 100)      # the fixed alert levels Forge sends today, in % of the app's allowance
BUDGET_AT = 125                 # apps with a developer-set budget: alert when usage reaches 125% of allowance


def app(i: int) -> str:
    return f"app_{i:05d}"


def part(key: str, partitions: int) -> int:
    # One partition per app, the same function in every script, so an app's usage stays in order.
    return zlib.crc32(key.encode()) % partitions


def cycle(cycle_s: float, now: float | None = None) -> int:
    """The billing cycle a moment falls in. The kit compresses a cycle into --cycle-s seconds; 0 means the
    calendar month (UTC), as Forge bills."""
    t = time.time() if now is None else now
    if cycle_s <= 0:
        g = time.gmtime(t)
        return g.tm_year * 12 + g.tm_mon - 1
    return int(t // cycle_s)


def cycle_seconds(cycle_s: float, now: float | None = None) -> float:
    if cycle_s > 0:
        return cycle_s
    g = time.gmtime(time.time() if now is None else now)
    return calendar.monthrange(g.tm_year, g.tm_mon)[1] * 86_400


class AppMix:
    """A skewed app mix: a few popular apps run most invocations, a long tail runs a few (illustrative)."""

    def __init__(self, apps: int):
        w = [1 / (i + 1) ** 0.8 for i in range(apps)]
        s = sum(w)
        self.names = [app(i) for i in range(apps)]
        self.share = {n: x / s for n, x in zip(self.names, w)}
        self.cum, acc = [], 0.0
        for x in w:
            acc += x / s
            self.cum.append(acc)

    def pick(self, r: float) -> str:
        return self.names[min(len(self.names) - 1, bisect.bisect_left(self.cum, r))]


def _frac(key: str) -> float:
    return (zlib.crc32(key.encode()) % 1000) / 1000


def allowance(name: str, share: float, rate: float, cycle_s: float) -> float:
    """An app's allowance for one compressed cycle, in GB-seconds (illustrative).

    Forge's real allowance is 200,000 GB-seconds per app per month. A demo cycle lasts seconds, so the kit
    scales it to each app's expected usage in a cycle times a factor between 0.4 and 3.0: about a fifth of
    apps pass 100% in a cycle, most cross 50%, and some never reach it."""
    expected = rate * MEAN_GBS * cycle_s * share
    return max(0.5, expected * (0.4 + 2.6 * _frac(name)))


def has_budget(name: str) -> bool:
    # About a third of apps carry a developer-set budget (illustrative: Forge has fixed alerts only today).
    return zlib.crc32((name + "/budget").encode()) % 3 == 0


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
