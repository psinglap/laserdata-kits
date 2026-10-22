"""Shared settings for the OpsAI command lane kit: kubectl commands from OpsAI to the agent in each customer
cluster, and their replies back, on Apache Iggy.

Today OpsAI reaches a cluster through Pulsar: the request goes on topic mcp-<token>-<sha1(cluster)>, the agent
in that cluster (synthetics-agent in MCP mode) runs kubectl, and posts the reply to <topic>-unsubscribe, where
OpsAI matches it by request ID (middleware-labs/synthetics-agent and mcp-kubernetes on GitHub). This kit is that
one lane: requests keyed by cluster, replies keyed by cluster, matched by request ID, and every request carries
the caller's deadline, which the agent checks before it runs anything.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyExpiry

STREAM = "opsai"
REQUESTS = "kubectl-requests"   # OpsAI -> the agent in each cluster, keyed by cluster
REPLIES = "kubectl-replies"     # agent -> OpsAI, keyed by cluster, matched by request ID
LOGS = "otlp-logs"              # a customer log burst on the same broker (the isolation test)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {REQUESTS: 16, REPLIES: 8, LOGS: 16}
# Requests and replies are kept 65 minutes, the expiration the agent already sets on its MCP replies.
RETENTION = IggyExpiry.ExpireDuration(timedelta(minutes=65))

# Command timeouts, in seconds, clamped as the agent clamps them today (k8s_executor.go: default 45, 5..300).
DEFAULT_TIMEOUT, MIN_TIMEOUT, MAX_TIMEOUT = 45, 5, 300


def clamp_timeout(s: float) -> float:
    return DEFAULT_TIMEOUT if s <= 0 else max(MIN_TIMEOUT, min(MAX_TIMEOUT, s))


def cluster(i: int) -> str:
    return f"acct{i % 97:02d}-k8s-{i:04d}"     # illustrative names; one account owns several clusters


def part(key: str, partitions: int) -> int:
    # One partition per cluster, the same function in every script, so a cluster's commands stay in order.
    return zlib.crc32(key.encode()) % partitions


# What an investigation sends, step by step (illustrative; OpsAI chooses its own commands).
# (command, kind, simulated kubectl time range in seconds)
TRIAGE = [("kubectl get pods -n {ns} -o wide", "read", (0.05, 0.30)),
          ("kubectl get events -n {ns} --sort-by=.lastTimestamp", "read", (0.05, 0.40)),
          ("kubectl top pods -n {ns}", "read", (0.10, 0.60)),
          ("kubectl describe deploy/{app} -n {ns}", "read", (0.05, 0.30))]
LOGS_CMD = ("kubectl logs deploy/{app} -n {ns} --tail=200", "read", (0.10, 1.50))
SLOW_LOGS = ("kubectl logs deploy/{app} -n {ns} --since=6h", "read", (3.0, 12.0))
FIXES = [("kubectl rollout restart deploy/{app} -n {ns}", "write", (0.2, 0.8)),
         ("kubectl scale deploy/{app} -n {ns} --replicas=6", "write", (0.2, 0.6)),
         ("kubectl set resources deploy/{app} -n {ns} --limits=memory=1Gi", "write", (0.2, 0.8))]
VERIFY = ("kubectl rollout status deploy/{app} -n {ns} --timeout=60s", "read", (1.0, 6.0))
APPS = [("checkout", "checkout-api"), ("payments", "payments-worker"), ("search", "search-api"),
        ("web", "web-frontend"), ("orders", "orders-consumer")]


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
                await client.create_topic(STREAM, t, PARTITIONS[t],
                                          message_expiry=RETENTION if t != LOGS else None)
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
