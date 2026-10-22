"""Shared settings for the Airtel domain verdict lane: every new verdict on a domain (block, or clean) from
Airtel's Fraud Detection Solution reaches every enforcement point that resolves or scans links (DNS resolver
nodes, the SMS link scanner) in milliseconds, on Apache Iggy, built by LaserData.

A message on the lane is the resulting verdict for one domain after a change, never a delta: block or clean,
the composite fraud score, which signal produced it (the SMS link scan, a partner API score, a threat feed,
a customer report), a per-domain sequence number and the time the verdict was set. Any node can start from
any message, and a node that starts late rebuilds every verdict by replaying the log.
"""
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "fraud-detection"
VERDICTS = "domain-verdicts"         # the lane: one message per verdict, keyed by domain, resulting verdict
RECORDS = "call-sms-records"         # a heavy, latency-tolerant feed on the same broker (call and SMS records)
PARTITIONS = {VERDICTS: 8, RECORDS: 16}
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=6))   # rebuilds and replay; a rolling snapshot keeps rebuilds short
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

BLOCK, CLEAN = "B", "C"
SOURCES = ["sms", "api", "feed", "rpt"]   # SMS link scan, partner API score, threat feed, customer report

# Illustrative domain names only: the shapes smishing links take (KYC, refunds, parcels, bills, rewards).
WORDS = ["kyc-update", "refund-claim", "parcel-redeliver", "bill-due", "reward-points", "loan-approve",
         "sim-verify", "power-bill", "tax-refund", "job-offer", "prize-claim", "account-unlock"]
TLDS = ["top", "xyz", "info", "online", "site", "in", "live", "click"]


def domain(n: int) -> str:
    return f"{WORDS[n % len(WORDS)]}-{n:06d}.{TLDS[(n // len(WORDS)) % len(TLDS)]}"


def domains(n: int) -> list:
    return [domain(i) for i in range(n)]


def part(key: str) -> int:
    # The same function in every script, so a domain's verdicts always land on one partition, in order.
    return zlib.crc32(key.encode()) % PARTITIONS[VERDICTS]


def msg(k: str, seq: int, verdict: str, score: float, src: str, waited_ns: int = 0) -> bytes:
    # Created time = now, minus how long the verdict has already waited for its write.
    return json.dumps({"k": k, "q": seq, "v": verdict, "sc": round(score, 3), "s": src,
                       "ns": time.time_ns() - waited_ns}, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topic(c: IggyClient, topic: str):
    """Create the stream and topic if they are missing, so any script can start first."""
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    if await c.get_topic(STREAM, topic) is None:
        try:
            await c.create_topic(STREAM, topic, PARTITIONS[topic], message_expiry=RETENTION)
        except RuntimeError:
            pass


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
