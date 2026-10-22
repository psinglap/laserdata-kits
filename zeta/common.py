"""Shared settings for the account spend lane kit: every approval and reversal on a primary or add-on card
reaches the transaction feature store before the account's next authorization, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "tachyon-auth"
SPEND = "account-spend"         # the lane: each approval or reversal as the account's resulting spend, keyed by account
CHECKS = "auth-checks"          # the decision engine's check on an account's next authorization
EVENTS = "tachyon-events"       # a heavy, latency-tolerant event-catalog stream (notifications, data lake) next door
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {SPEND: 16, CHECKS: 16, EVENTS: 32}

# Spend categories the feature store keeps per account. Zeta's Tachyon page says the feature store can
# "track real-time spend for each account based on attributes like MCC, category, location, merchant, or card";
# these MCC groups are illustrative.
CATEGORIES = ("grocery", "fuel", "dining", "travel", "online", "cash")


def account_id(n: int) -> str:
    return f"A{n:010d}"         # illustrative account IDs


def card_id(account: str, k: int) -> str:
    return f"{account}-{'P' if k == 0 else 'AO' + str(k)}"   # P = primary card, AO1..AO3 = family add-on cards


def part(account: str, partitions: int) -> int:
    # One partition per account, the same function in every script, so an account's spend stays in order.
    return zlib.crc32(account.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


class Spend:
    """Today's spend per account and category, in paise, across the primary card and every add-on card.

    Used by switch.py (the source of truth in the kit) and by kafka_bridge.py (on your real authorization
    outcomes), so the lane always carries the resulting spend, never a delta: a reversal applied twice, or
    an approval replayed, cannot move the total. The kit keeps every account it has seen; a real store
    resets at the cycle boundary.
    """

    def __init__(self):
        self.spend = {}                         # account -> {category: paise}
        self.seq = {}                           # account -> sequence of the account's last change

    def apply(self, account: str, category: str, delta: int) -> dict:
        s = self.spend.setdefault(account, {})
        s[category] = max(0, s.get(category, 0) + delta)
        self.seq[account] = n = self.seq.get(account, 0) + 1
        return dict(account=account, seq=n, spend=dict(s))

    def get(self, account: str, category: str) -> int:
        return self.spend.get(account, {}).get(category, 0)


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
