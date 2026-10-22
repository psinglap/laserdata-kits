"""Shared settings for the payment downtime lane kit: payment outcomes in, downtime signals out, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "payment-health"
STATUS = "payment-status"        # the lane in: one event per payment, its final status, keyed by cohort
SIGNALS = "downtime-signals"     # the lane out: each cohort's downtime state, read by Checkout and webhooks
WORKFLOW = "workflow-events"     # a heavy, latency-tolerant neighbour: every workflow event, for the data lake
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"
PARTITIONS = {STATUS: 16, SIGNALS: 4, WORKFLOW: 16}

# Payment cohorts, named the way Razorpay's public Downtime API names instruments:
# UPI by PSP handle, cards by issuer and card type, netbanking by bank code: 50 cohorts here.
# Razorpay's 2021 post detects downtimes on "50+ payment cohorts at the Razorpay level".
UPI = ["ybl", "okhdfcbank", "okaxis", "oksbi", "okicici", "paytm", "axl", "ibl", "upi", "apl"]
ISSUERS = ["HDFC", "SBIN", "ICIC", "UTIB", "KKBK", "INDB", "YESB", "RATN", "IDFB", "BARB"]
BANKS = ["SBIN", "HDFC", "ICIC", "UTIB", "KKBK", "PUNB", "BARB", "CNRB", "UBIN", "YESB",
         "IDIB", "BKID", "IOBA", "INDB", "FDRL", "IDFB", "AUBL", "KARB", "CSBK", "SIBL"]


def cohorts():
    """(cohort key, share of payments, usual success rate). Shares and rates are illustrative."""
    out = []
    for i, h in enumerate(UPI):
        out.append((f"upi:psp:{h}", 0.60 * 0.42 / (i + 1) ** 0.9, 0.88))
    for i, b in enumerate(ISSUERS):
        for t, s in (("credit", 0.55), ("debit", 0.45)):
            out.append((f"card:issuer:{b}:{t}", 0.25 * 0.33 * s / (i + 1) ** 0.9, 0.82))
    for i, b in enumerate(BANKS):
        out.append((f"netbanking:bank:{b}", 0.15 * 0.28 / (i + 1) ** 0.9, 0.78))
    total = sum(w for _, w, _ in out)
    return [(k, w / total, sr) for k, w, sr in out]


def part(cohort: str, partitions: int) -> int:
    # One partition per cohort, the same function in every script, so a cohort's payments stay in order.
    return zlib.crc32(cohort.encode()) % partitions


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
