"""Shared settings for the concurrency lane kit: every conversation start and end reaches the shared plan-ceiling
ledger in milliseconds, and every change in an organization's headroom reaches every voice-agent worker, on
Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "samvaad"
SLOTS = "slot-events"           # the lane: each worker's live conversations per organization, keyed by organization
HEADROOM = "org-headroom"       # the ledger's decisions back to every worker: open, outbound full, full
TRANSCRIPTS = "call-transcripts"  # a heavy, latency-tolerant stream on the same broker: post-call payloads
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {SLOTS: 16, HEADROOM: 1, TRANSCRIPTS: 32}

OPEN, OUT_FULL, FULL = "open", "outbound_full", "full"
INBOUND_SHARE = 0.2             # share of conversations that are inbound calls (illustrative; set yours)
INSTANT_SHARE = 0.1             # share that are instant outbound calls; the rest are campaign calls
LEASE_S = 3.0                   # a worker silent this long has its slots released (it crashed or was drained)


def org(i: int) -> str:
    return f"org_{i:04d}"


def part(key: str, partitions: int) -> int:
    # One partition per organization, the same function in every script, so an org's slot events stay in order.
    return zlib.crc32(key.encode()) % partitions


def _frac(key: str) -> float:
    return (zlib.crc32(key.encode()) % 1000) / 1000


def weight(o: str) -> float:
    # Some organizations run far more calls than others (illustrative spread of 1 to 10).
    return 1 + 9 * _frac(o + "/size")


def plan(o: str, conv_per_s: float, talk_s: float, orgs: int) -> tuple[int, int]:
    """An organization's plan ceiling on live conversations, and the part of it kept for inbound calls.

    Like Sarvam's plan limits, the ceiling is shared by every campaign, instant outbound and inbound call, and
    inbound capacity is reserved explicitly (here 20% of the ceiling). Values are illustrative: about a third of
    organizations have a ceiling well above what they run and never reach it; the rest run campaigns that would
    hold more live conversations than their ceiling allows, so the ceiling bites while you watch."""
    total_w = sum(weight(org(i)) for i in range(orgs))
    demand = conv_per_s * weight(o) / total_w * talk_s          # live conversations this org would hold
    h = zlib.crc32(o.encode())
    f = 2.5 if h % 3 == 0 else 0.75 + 0.3 * _frac(o + "/plan")
    ceiling = max(5, round(demand * f))
    return ceiling, max(1, round(ceiling * 0.2))


def state_of(live: int, ceiling: int, reserve: int) -> str:
    if live >= ceiling:
        return FULL                 # Sarvam's docs: past a ceiling, new calls are queued or rejected
    if live >= ceiling - reserve:
        return OUT_FULL             # outbound has used its share; what is left is kept for inbound
    return OPEN


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
