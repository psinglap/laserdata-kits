"""Shared settings for the Insights witness lane: every upload batch an Insights Agent sends reaches
witness_assembler in milliseconds, keyed by service, and every batch is accounted for by agent and number,
on Apache Iggy."""
import asyncio
import bisect
import json
import time
import zlib

STREAM = "insights"
WITNESSES = "witness-batches"   # the lane: one message per agent upload batch, keyed by Insights service
TELEMETRY = "agent-telemetry"   # a heavy, latency-tolerant stream on the same broker: agent capture diagnostics
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {WITNESSES: 16, TELEMETRY: 32}
FLUSH_S = 5.0                   # the agent flushes its upload batch every 5 s (uploadBatchFlushDuration)

# Services the agents watch. identity-service appears in Postman's own agent pull request #200; the rest of
# the names, the endpoints and the traffic mix are illustrative. Services past this list are numbered.
NAMES = ["identity-service", "workspace-service", "collection-service", "sync-service", "monitor-runner",
         "mock-server", "search-service", "notification-service", "billing-service", "api-catalog",
         "flows-runtime", "team-service", "audit-service", "spec-hub", "public-api", "webhook-service"]
PATHS = ["GET /v1/{0}/{{id}}", "GET /v1/{0}", "POST /v1/{0}", "PUT /v1/{0}/{{id}}", "DELETE /v1/{0}/{{id}}",
         "GET /v1/{0}/{{id}}/members", "POST /v1/{0}/{{id}}/events", "GET /v1/{0}/search", "GET /health",
         "POST /v1/{0}/batch", "GET /v1/{0}/{{id}}/history", "PATCH /v1/{0}/{{id}}"]


def service(i: int) -> str:
    return f"svc_{NAMES[i]}" if i < len(NAMES) else f"svc_service-{i:03d}"


def endpoints(name: str) -> list:
    noun = name.removeprefix("svc_").split("-")[0] + "s"
    return [p.format(noun) for p in PATHS]


def part(key: str, partitions: int) -> int:
    # One partition per service, the same function in every script, so an agent's batches stay in order.
    return zlib.crc32(key.encode()) % partitions


class Mix:
    """A skewed mix: a few busy services carry most traffic, a long tail carries a little (illustrative)."""

    def __init__(self, names: list, skew: float = 0.8):
        w = [1 / (i + 1) ** skew for i in range(len(names))]
        s = sum(w)
        self.names = names
        self.share = {n: x / s for n, x in zip(names, w)}
        self.cum, acc = [], 0.0
        for x in w:
            acc += x / s
            self.cum.append(acc)

    def pick(self, r: float) -> str:
        return self.names[min(len(self.names) - 1, bisect.bisect_left(self.cum, r))]


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
        out = {"n": len(self.samples), "p50": pick(0.5), "p90": pick(0.9), "p99": pick(0.99),
               "p999": pick(0.999), "max": s[-1]}
        self.samples = []
        return out
