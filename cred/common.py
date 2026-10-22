"""Shared settings for the CRED payment status lane kit: every payment status change and balance update for a
member (UPI payments, credit card bill payments, cashback, CRED coins, reward redemptions, CRED Win prizes), keyed
by member, to every Propeller node in milliseconds, and replayed from the log to a device that reconnects, on
Apache Iggy.

An event on the lane carries the member, a per-member sequence number, the payment or reward reference, the flow,
the resulting status (and the failure reason, when there is one) and, for CRED coins, the resulting balance, never
only a delta, plus the time the backend created it. Event names, statuses and reasons are illustrative: CRED has
not published its event schema. Propeller (github.com/CRED-CLUB/propeller) identifies a member by the x-user-id
header and each of the member's devices by x-device-id; the kit uses the same two keys.
"""
import asyncio
import json
import time
import zlib

from apache_iggy import IggyClient

STREAM = "propeller"
EVENTS = "member-events"         # the lane: one record per status change or balance update, keyed by member
EXPORT = "recon-export"          # a heavy, latency-tolerant neighbour (payment and payout records for reconciliation)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {EVENTS: 4, EXPORT: 8}
FINAL = {"SUCCESS", "FAILED", "BILL_PAID", "REFUNDED"}     # a payment's last status: what the member waits for


def member(i: int) -> str:
    # The x-user-id Propeller reads. Illustrative format.
    return f"mbr_{7340001 + i}"


def devices_of(k: str) -> list[str]:
    # The x-device-id values of a member's devices: one in five members has the app open on a second device.
    n = 2 if zlib.crc32(b"dev" + k.encode()) % 5 == 0 else 1
    return [f"{k}:d{j + 1}" for j in range(n)]


def part_of(k: str, partitions: int = PARTITIONS[EVENTS]) -> int:
    # The one partition function used everywhere: a member's events always land in one partition, in order.
    return zlib.crc32(k.encode()) % partitions


def node_of(device: str, nodes: int) -> int:
    # Which Propeller node holds a device's channel. The load balancer picks it at connect, and a member's two
    # devices can be on two nodes, so every node reads every partition.
    return zlib.crc32(b"node" + device.encode()) % nodes


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


async def ensure_topics(c: IggyClient, *topics):
    """Create the stream and topics if they are missing, so any script can start first."""
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    for t in topics:
        if await c.get_topic(STREAM, t) is None:
            try:
                await c.create_topic(STREAM, t, PARTITIONS[t])
            except RuntimeError:
                pass
    while not all([await c.get_topic(STREAM, t) for t in topics]):
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
