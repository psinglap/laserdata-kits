"""Shared settings for the Computer credit lane: every agent action's credits reach the credit ledger in
milliseconds, and every pause and resume reaches every agent runner, on Apache Iggy."""
import asyncio
import json
import math
import time
import zlib

STREAM = "computer"
ACTIONS = "agent-actions"       # the lane: each runner's running credits per account, keyed by account
DECISIONS = "credit-decisions"  # the ledger's decisions back to every agent runner: pause, resume
TRACES = "task-traces"          # a heavy, latency-tolerant stream on the same broker: agent trajectories for evals
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {ACTIONS: 16, DECISIONS: 1, TRACES: 32}

CREDITS_PER_USD = 100           # Perplexity's help center: 100 credits = $1
# Credits for one agent action (a model call, a search, a browser step, a sandbox run): 2-40 (illustrative; set yours).
ACT_LOW, ACT_HIGH = 2, 40
AVG_ACT = (ACT_LOW + ACT_HIGH) / 2
KINDS = ("model", "search", "browser", "sandbox")


def account(i: int) -> str:
    return f"acct_{i:06d}"


def part(key: str, partitions: int) -> int:
    # One partition per account, the same function in every script, so an account's actions stay in order.
    return zlib.crc32(key.encode()) % partitions


def period(period_s: float, now: float | None = None) -> int:
    # The kit compresses a month into --period-s seconds; every script derives it from the clock.
    return int((time.time() if now is None else now) // period_s)


def _frac(key: str) -> float:
    return (zlib.crc32(key.encode()) % 1000) / 1000


def activity(a: str) -> float:
    """How busy an account's agents are, 0.25x to 4x (log-uniform), so some accounts run out and most don't."""
    return 0.25 * 16 ** _frac(a + "/activity")


MEAN_ACTIVITY = 3.75 / math.log(16)     # mean of the log-uniform 0.25..4


def plan(a: str, actions_per_s: float, accounts: int, period_s: float) -> dict:
    """An account's credit plan for the kit, in credits (illustrative shapes after Perplexity's published plans).

    even = what an average account spends in one compressed month.
      * pro: no monthly credits, a one-time bonus; half have auto-refill on (no spending cap).
      * max: monthly credits and a bonus, auto-refill on, and a monthly spending cap on refills.
      * enterprise: monthly credits per seat, no auto-refill.
    Credits are spent bonus first, then monthly ("plan"), then purchased."""
    even = actions_per_s * AVG_ACT * period_s / accounts
    h = zlib.crc32(a.encode()) % 10
    if h < 4:
        refill = h % 2 == 0
        return {"plan": "pro", "monthly": 0, "bonus": round(2 * even), "threshold": round(0.1 * even) if refill else 0,
                "refill": round(0.5 * even) if refill else 0, "cap": round(4 * even) if refill else 0}
    if h < 8:
        return {"plan": "max", "monthly": round(even), "bonus": round(0.5 * even), "threshold": round(0.25 * even),
                "refill": round(0.5 * even), "cap": round(even)}
    return {"plan": "enterprise", "monthly": round(1.5 * even), "bonus": 0, "threshold": 0, "refill": 0, "cap": 0}


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
