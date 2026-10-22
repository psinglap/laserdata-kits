"""Shared settings for the app wallet lane kit: every message's fees reach the app's wallet in milliseconds, and
every wallet state change (low balance, 1003 blocked, recharged) reaches every send API node, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "wallet"
CHARGES = "charges"           # the lane: each send node's running fees per app (and recharges), keyed by app
STATE = "wallet-state"        # the wallet service's state changes back to every send API node: low, blocked, ok
INBOUND = "inbound-messages"  # a heavy, latency-tolerant stream on the same broker: inbound user messages and media
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {CHARGES: 16, STATE: 1, INBOUND: 32}

# Fees per message in USD (illustrative; set your rate card). The Gupshup fee is charged when a message is
# accepted; the WhatsApp (Meta) fee when it is delivered, as Gupshup's partner release notes describe.
GS_FEE = 0.001
CATEGORIES = {                 # category: (share of outbound messages, Meta fee per delivered message)
    "marketing": (0.45, 0.0118),
    "utility": (0.35, 0.0016),
    "authentication": (0.15, 0.0016),
    "service": (0.05, 0.0040),  # Meta charges service messages from 1 Oct 2026; rate illustrative
}
DELIVERED = 0.92               # share of accepted messages that are delivered (and so carry the Meta fee)
AVG_COST = GS_FEE + DELIVERED * sum(s * f for s, f in CATEGORIES.values())


def app(i: int) -> str:
    return f"app-{i:05d}"


def part(key: str, partitions: int) -> int:
    # One partition per app, the same function in every script, so an app's charges stay in order.
    return zlib.crc32(key.encode()) % partitions


def shares(n: int) -> list[float]:
    """Each app's share of outbound messages: a few large brands and a long tail (illustrative)."""
    w = [1 / (i + 20) ** 0.9 for i in range(n)]
    t = sum(w)
    return [x / t for x in w]


def wallet(i: int, n: int, msgs_per_s: float, runway_s: float) -> dict:
    """An app's opening balance, overdraft and low-balance threshold, compressed so apps reach them while you watch.

    Opening balance = runway_s seconds of the app's expected spend, times 0.5-2. One app in four is
    well funded (50 times the runway) and never runs out. The overdraft is a quarter of the opening balance
    (Gupshup lets a partner set it between 0 and a limit it calculates from usage); the low-balance alert fires
    at a fifth of it (in production, the $5 email). Two apps in three recharge after they are blocked."""
    a = app(i)
    h = zlib.crc32(a.encode())
    spend = msgs_per_s * shares(n)[i] * AVG_COST
    factor = 50.0 if h % 4 == 0 else 0.5 + 1.5 * ((h >> 8) % 1000) / 1000
    opening = round(spend * runway_s * factor, 4)
    return {"opening": opening, "overdraft": round(opening / 4, 4), "low": round(opening / 5, 4),
            "recharges": h % 3 != 0}


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
