"""Shared settings for the broadcast push lane kit: eBay Event Subscription Platform notifications on Apache Iggy.

One broadcast event (sent to every subscription on its topic) is split into window tasks of --window-size
subscriptions, ordered by creation time, as eBay's Notification Engineering team describes; each window task goes
to the partition window % partitions, so the push worker that owns a window always serves it and keeps its
subscriptions cached. Unicast notifications (one subscription each) run on their own topic, keyed by subscription,
and are never queued behind a broadcast. A push whose endpoint does not acknowledge goes to its own retry topic.
"""
import asyncio
import json
import time
import zlib

STREAM = "event-subscription"
UNICAST = "unicast-notifications"   # one notification for one subscription, keyed by subscription
TASKS = "broadcast-windows"         # one task per (broadcast event, subscription window), partition = window % N
RETRY = "push-retry"                # pushes whose endpoint did not acknowledge, keyed by subscription
RECEIPTS = "window-receipts"        # one receipt per window pushed, for the broadcast completion tracker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {UNICAST: 16, TASKS: 16, RETRY: 8, RECEIPTS: 1}

# Topic names from eBay's public Notification API documentation. Which topics eBay sends as broadcasts, and to how
# many subscriptions, is not public; MARKETPLACE_ACCOUNT_DELETION (every app must subscribe or opt out) is used here
# as the illustrative broadcast, and the subscription count comes from eBay's post (20,000 subscribers).
UNICAST_TOPICS = ("LISTING", "ITEM_AVAILABILITY", "ITEM_PRICE_REVISION", "SELLER_STANDARDS_PROFILE_METRICS")
BROADCAST_TOPIC = "MARKETPLACE_ACCOUNT_DELETION"


def sub_id(i: int) -> str:
    return f"sub-{i:07d}"


def part(k: str, partitions: int) -> int:
    # The same function in every script, so one subscription's notifications stay in order.
    return zlib.crc32(k.encode()) % partitions


def windows(subs: int, size: int) -> int:
    return (subs + size - 1) // size


def window_subs(w: int, subs: int, size: int) -> range:
    """Subscription indexes in window w. Subscriptions are ordered by their immutable creation time, so a window
    never changes once full and any worker can rebuild it from (w, size); here index order stands in for it."""
    return range(w * size, min(subs, (w + 1) * size))


def endpoint_down(i: int, t: float, share: float, up_s: float, down_s: float) -> bool:
    """The kit's planted endpoint outages (ground truth, shared by every script): a --down-share of subscription
    endpoints alternate between --down-s seconds of not acknowledging and --up-s seconds of acknowledging."""
    h = zlib.crc32(f"ep{i}".encode())
    if (h % 10_000) >= share * 10_000:
        return False
    period = up_s + down_s
    return ((t + (h >> 8) % 1000 / 1000 * period) % period) < down_s


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
