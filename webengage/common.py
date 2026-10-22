"""Shared settings for the Contextual Journeys trip lane kit.

A Contextual Journey runs one trip per user per context ("User + Context = Unique Journey Trip",
WebEngage, Dec 2025): one trip per Policy ID, Loan ID or Category ID. Each trip is entered by an
event, waits, and sends a reminder unless the context's exit event (Policy Renewed for the same
policy) arrives first.

Every tracked event goes to one Apache Iggy topic, keyed by user ID: all of a user's events, for all
of the user's trips, land on the same partition, in order. Each journey worker owns a fixed set of
partitions and reads them directly, so a user's trips are only ever advanced by one worker, one
event at a time. Reminders the workers decide to send go to a sends topic, keyed the same way, which
the send side (here send_audit.py) reads.
"""
import asyncio
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "journeys"
EVENTS_TOPIC = "tracked-events"                    # every tracked event, keyed by user ID
SENDS_TOPIC = "journey-sends"                      # reminders the journey workers queued, keyed by user ID
CAMPAIGN_STREAM = "campaigns"                      # the one-time campaign send next door (campaign_blast.py)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))          # a day of events to replay or audit

# Three Contextual Journeys, with the context attributes, entry events and exit events from
# WebEngage's own examples (loan, policy renewal, browse abandonment). Event names are illustrative.
JOURNEYS = {
    "policy-renewal": dict(ctx="policy_id", entry="Renewal Due", exit="Policy Renewed",
                           activity=("Policy Viewed", "Renewal Quote Viewed"), reminder="renewal_reminder",
                           share=40),
    "loan-application": dict(ctx="loan_id", entry="Loan Application Started", exit="Payment Successful",
                             activity=("Documents Uploaded", "KYC Completed"), reminder="loan_reminder",
                             share=25),
    "browse-abandonment": dict(ctx="category_id", entry="Category Viewed", exit="Added To Cart",
                               activity=("Product Viewed",), reminder="browse_reminder", share=35),
}
# event name -> (journey, role), so a worker knows what each event does to a trip
ROLE = {}
for _j, _d in JOURNEYS.items():
    ROLE[_d["entry"]] = (_j, "entry")
    ROLE[_d["exit"]] = (_j, "exit")
    for _a in _d["activity"]:
        ROLE[_a] = (_j, "activity")
EXIT_NAMES = {d["exit"] for d in JOURNEYS.values()}


def user_id(n: int) -> str:
    return f"u{n:09d}"


def part(user: str, partitions: int) -> int:
    # The same function everywhere: one user, one partition, one owner, for all of the user's trips.
    return zlib.crc32(user.encode()) % partitions


def owner(pid: int, workers: int) -> int:
    # Static ownership: partition p belongs to worker p % workers. No lock, no lease, no rebalance.
    return pid % workers


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


async def ensure_topic(c: IggyClient, stream: str, topic: str, partitions: int, expiry=RETENTION):
    """Create the stream and topic if they are missing, so any script can start first."""
    if await c.get_stream(stream) is None:
        try:
            await c.create_stream(stream)
        except RuntimeError:
            pass                                    # created by another process in the meantime
    if await c.get_topic(stream, topic) is None:
        try:
            await c.create_topic(stream, topic, partitions, message_expiry=expiry)
        except RuntimeError:
            pass
    while not await c.get_topic(stream, topic):
        await asyncio.sleep(0.2)


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
