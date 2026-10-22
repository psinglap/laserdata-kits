"""Shared settings for the streaming aggregation lane kit: every metric sample, keyed by its shard key, reaches the
streaming aggregator that owns it in milliseconds, and every timeslice closes exact, counted once even when a
sample is sent twice, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "telemetry"
SAMPLES = "samples"                # the lane: raw metric samples, keyed by shard key (cluster, metric, shard_label)
AGGREGATES = "aggregates"          # the aggregated series, one resulting value per group per timeslice
TRUTH = "window-truth"             # the gateway's own count of what each group gained in each timeslice (the check)
LOGS = "logs"                      # a heavy, latency-tolerant stream on the same broker: raw log lines
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {SAMPLES: 16, AGGREGATES: 1, TRUTH: 1, LOGS: 32}

# The rule the aggregator runs, written the way a Last9 streaming aggregation is (PromQL, a function, an output
# name). sum2 is the reset-aware sum for counters: a sample lower than the one before is a counter reset.
METRIC = "http_requests_duration_seconds_bucket"
SHARD_LABEL = "service"            # the per-metric shard_label: one metric spread by service, not held on one node
RULE = f"sum2 by (cluster, service, le) ({METRIC}[timeslice])"
OUTPUT = f"{METRIC}:by_service"

SERVICES = ["checkout", "payments", "search", "catalog", "cart", "auth", "orders", "notifications", "inventory",
            "pricing", "reviews", "recommendations", "profile", "shipping", "coupons", "wallet", "gateway", "media",
            "chat", "ledger"]
BUCKETS = ["0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"]


def cluster(i: int) -> str:
    return f"prod-{i:02d}"


def buckets(n: int) -> list:
    n = max(2, min(n, len(BUCKETS)))
    return BUCKETS[:n - 1] + ["+Inf"]


def shard_key(c: str, service: str) -> str:
    # Last9's default shard key is cluster + metric name; the shard_label adds the service.
    return f"{c}|{METRIC}|{service}"


def part(key: str, partitions: int) -> int:
    # One partition per shard key, the same function in every script, so one aggregator owns each group.
    return zlib.crc32(key.encode()) % partitions


def window(t_ms: int, timeslice_s: float) -> int:
    return int(t_ms // int(timeslice_s * 1000))


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
