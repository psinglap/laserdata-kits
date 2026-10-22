"""Shared settings for the Flipkart Ads spend lane: every billable view and click reaches the budget controller
(BCAP) in milliseconds, and every budget stop reaches every ad server, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "flipkart-ads"
BILLABLE = "billable-events"     # the lane: billable views and clicks, keyed by campaign, with watermark marks
STOPS = "budget-stops"           # BCAP's stop decisions back to every ad server
TELEMETRY = "ui-telemetry"       # the rest of the UI telemetry firehose: heavy, latency-tolerant, same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {BILLABLE: 16, STOPS: 1, TELEMETRY: 32}

# Your rules, from "Guaranteeing Ad Budget Precision at 1 Million Events/Second" (Flipkart Tech Blog, Jan 2026):
BUCKET_S = 60                    # the dedupe key is the canonical impression ID plus a 1-minute event-time bucket
MAX_LATE_S = 3600                # events arriving more than an hour after their event time are stale and dropped


def campaign(i: int) -> str:
    return f"cmp-{i:05d}"


def part(key: str, partitions: int) -> int:
    # One partition per campaign, the same function in every script, so a campaign's events stay in order and
    # one BCAP reader sees all of them.
    return zlib.crc32(key.encode()) % partitions


IST_OFFSET_S = 19_800            # budget days roll over at midnight IST


def day(day_s: float, now: float | None = None) -> int:
    # The kit compresses a budget day into --day-s seconds; every script derives it from the clock.
    # With --day-s 86400 (a real day) it rolls over at midnight IST.
    return int(((time.time() if now is None else now) + IST_OFFSET_S) // day_s)


# Illustrative prices, in rupees (set them to yours): CPM-billed campaigns pay for viewable impressions,
# CPC-billed campaigns pay for clicks. About one campaign in four is CPC.
CPM_LOW, CPM_HIGH = 60.0, 400.0          # ₹ per 1,000 viewable impressions
CPC_LOW, CPC_HIGH = 2.0, 14.0            # ₹ per click


def is_cpc(c: str) -> bool:
    return zlib.crc32(c.encode()) % 4 == 0


def avg_cost() -> float:
    return 0.75 * (CPM_LOW + CPM_HIGH) / 2 / 1000 + 0.25 * (CPC_LOW + CPC_HIGH) / 2


def budget(c: str, events_per_s: float, campaigns: int, day_s: float) -> float:
    """Each campaign's daily budget in ₹ (illustrative). About a third are always-on with a large budget, so
    traffic never stops; the rest get 30-100% of an even share and run out during the day."""
    even = events_per_s * avg_cost() * day_s / campaigns
    h = zlib.crc32(c.encode())
    return round(even * (6.0 if h % 3 == 0 else 0.3 + 0.7 * (h % 1000) / 1000), 2)


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
