"""Shared settings for the playback health lane kit: heartbeats in, CDN health signals out, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "playback-health"
HEARTBEATS = "heartbeats-lite"   # the lane in: minimized heartbeats (~1.1 KB), keyed by cohort and CDN
SIGNALS = "cdn-health"           # the lane out: each cohort-CDN pair's health state, read by routing and alerting
FULL = "heartbeats-full"         # a heavy, latency-tolerant neighbour: full ~7.5 KB heartbeats for full-data consumers
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"
PARTITIONS = {HEARTBEATS: 16, SIGNALS: 4, FULL: 16}

# Cohorts the way JioHotstar's traffic-orchestration post (Mar 2026) describes them: network plus
# geography (its A/B ran on ASN-State cohorts). Networks, states, CDN names and shares are illustrative.
NETWORKS = [("jio", 0.45), ("airtel", 0.33), ("vi", 0.12), ("bsnl", 0.04), ("act", 0.03), ("hathway", 0.03)]
STATES = ["MH", "UP", "KA", "DL", "TN", "WB", "GJ", "RJ"]
CDNS = [("cdn-a", 0.40), ("cdn-b", 0.35), ("cdn-c", 0.25)]


def cohorts():
    """(cohort, share of viewers): 48 network-state cohorts."""
    out = []
    for n, w in NETWORKS:
        for i, s in enumerate(STATES):
            out.append((f"{n}:{s}", w / (i + 1) ** 0.8))
    total = sum(w for _, w in out)
    return [(c, w / total) for c, w in out]


def pairs():
    """(cohort|cdn key, share of heartbeats): every cohort on every CDN, 144 keys."""
    return [(f"{c}|{cdn}", w * cw) for c, w in cohorts() for cdn, cw in CDNS]


def part(key: str, partitions: int) -> int:
    # One partition per key, the same function in every script, so a key's heartbeats stay in order.
    return zlib.crc32(key.encode()) % partitions


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
