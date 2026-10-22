"""Shared settings for the Kill Bill usage lane kit: every payment's billable usage (txnCount and txnVolume), keyed
by subscription, reaches the subscription's live usage total in milliseconds, counted once even when it is sent
twice, and waits in the log, nothing lost, while Kill Bill is down. On Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "billing"
USAGE = "usagemetrics"           # the lane: one usage message per payment, keyed by subscriptionExternalKey
DAILY = "usage-daily"            # per-subscription usage for the day so far, the resulting totals, for live views
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {USAGE: 16, DAILY: 1}
IST = 19_800                     # seconds east of UTC; recordDate is the IST calendar day


def subscription(i: int) -> str:
    return f"sub_{i:07d}"        # the external key the subscription was created with in Kill Bill


def tenant(sub: str) -> str:
    return f"tenant-{zlib.crc32(sub.encode()) % 4}"


def part(key: str, partitions: int) -> int:
    # One partition per subscription, the same function in every script, so a subscription's usage stays in order.
    return zlib.crc32(key.encode()) % partitions


def record_date(ns: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ns / 1e9 + IST))


def usage_msg(sub, tracking_id, day, volume_paise, seq, cnt, vol, ns=None) -> bytes:
    """The Kill Bill Kafka consumer plugin's usage message, plus three lane fields:
    seq (per subscription), cnt and vol (the subscription's txnCount and txnVolume for the day so far, the
    resulting totals). Kill Bill reads the records; the lane's readers use seq and the totals to count once."""
    return json.dumps({
        "subscriptionExternalKey": sub, "trackingId": tracking_id, "tenantId": tenant(sub),
        "unitUsageRecords": [
            {"unitType": "txnCount", "usageRecords": [{"recordDate": day, "amount": 1}]},
            {"unitType": "txnVolume", "usageRecords": [{"recordDate": day, "amount": volume_paise}]}],
        "seq": seq, "cnt": cnt, "vol": vol, "ns": ns or time.time_ns()}, separators=(",", ":")).encode()


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


def amounts(u: dict):
    """(recordDate, txnCount, txnVolume in paise) from a usage message's unitUsageRecords."""
    day, cnt, vol = None, 0, 0
    for unit in u["unitUsageRecords"]:
        for r in unit["usageRecords"]:
            day = r["recordDate"]
            if unit["unitType"] == "txnCount":
                cnt += r["amount"]
            elif unit["unitType"] == "txnVolume":
                vol += r["amount"]
    return day, cnt, vol


def rupees(paise: float) -> str:
    return f"₹{paise / 100:,.0f}"


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
