"""Shared settings for the SIEM outage lane: Cribl Stream's security events in one Apache Iggy log.

A Worker Group writes the events of one Route (the security events bound for the SIEM tier) once,
to one topic, keyed by the sending host. Every Destination behind it (the SIEM, Cribl Lake, a new
SIEM during a migration) reads that topic directly, at its own pace, from its own offsets. When a
SIEM is down, its events wait in the log, not on a Worker Process's disk, and no other Destination
and no Source has to slow down.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "cribl_stream"
TOPIC = "security_events"                          # the Route's output, written once for every Destination
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=72))          # a weekend-long SIEM outage, with room to spare

# Sending hosts by kind and the sourcetype each one sends (illustrative mix for a security Route).
KINDS = (("fw", "firewall", 30), ("dc", "windows_security", 20), ("dns", "dns", 18), ("proxy", "web_proxy", 14),
         ("edr", "endpoint", 12), ("lnx", "linux_auth", 6))


def hosts(n: int, node: int = 0) -> list:
    """n sending hosts for one Worker Node, named by kind, in the mix above."""
    total = sum(w for _, _, w in KINDS)
    out = []
    for prefix, st, w in KINDS:
        for i in range(max(1, round(n * w / total))):
            out.append((f"{prefix}-{node:02d}{i:04d}", st))
    return out


def part(host: str, partitions: int) -> int:
    # Keyed by host, so one sender's events stay in order on one partition. Same function in every script.
    return zlib.crc32(host.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topic(c: IggyClient, stream: str, topic: str, partitions: int):
    """Create the stream and topic if they are missing, so any script can start first."""
    if await c.get_stream(stream) is None:
        try:
            await c.create_stream(stream)
        except RuntimeError:
            pass                                    # created by another process in the meantime
    if await c.get_topic(stream, topic) is None:
        try:
            await c.create_topic(stream, topic, partitions, message_expiry=RETENTION)
        except RuntimeError:
            pass
    while not await c.get_topic(stream, topic):
        await asyncio.sleep(0.2)


async def ends(c: IggyClient, stream: str, topic: str) -> list:
    """The next offset to be written on every partition: where a reader that wants only new events starts."""
    t = await c.get_topic(stream, topic)
    return [p.current_offset + 1 if p.messages_count else 0 for p in sorted(t.partitions, key=lambda p: p.id)]


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
