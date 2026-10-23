"""Shared settings for the identity enforcement lane kit: every authentication a domain controller sees reaches the
identity detection engine in milliseconds, and every block reaches every domain controller before the account's
next authentication, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "identity-protection"
AUTH = "auth-events"          # the lane: one record per authentication a DC sensor sees, keyed by account
ACCOUNT = "account-events"    # account events from outside the auth path, such as a compromised password
ENFORCE = "identity-enforcement"   # the detection engine's blocks and MFA steps, read by every DC sensor
TELEMETRY = "sensor-telemetry"     # a heavy, latency-tolerant stream on the same broker: endpoint telemetry
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {AUTH: 16, ACCOUNT: 1, ENFORCE: 1, TELEMETRY: 32}
PROTOCOLS = ("kerberos", "ntlm", "ldap")


def account(i: int) -> str:
    # Accounts of many customers share the lane; the customer ID is part of the key (illustrative names).
    return f"cid{zlib.crc32(str(i).encode()) % 400:03d}/u{i:06d}"


def own_hosts(acct: str) -> tuple[list[str], list[str]]:
    """Where an account normally authenticates: from its one or two workstations, to a handful of servers."""
    h = zlib.crc32(acct.encode())
    src = [f"ws-{h % 90000:05d}", f"ws-{(h // 90000) % 90000:05d}"]
    dst = [f"srv-{(h >> (4 * j)) % 4000:04d}" for j in range(4)]
    return src, dst


def part(acct: str, partitions: int) -> int:
    # One partition per account, the same function in every script, so an account's authentications stay in order.
    return zlib.crc32(acct.encode()) % partitions


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
