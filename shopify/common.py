"""Shared settings for the buyer signal lane: every cart change and completed order of a buyer who is chatting
with a merchant reaches that conversation in Shopify Inbox before the next reply, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "shopify-inbox"
SIGNALS = "buyer-signals"        # the lane: cart adds and removes, checkout started, order completed, keyed by buyer
REPLIES = "inbox-replies"        # the next reply in the buyer's conversation, from the Inbox agent or the merchant's staff
STOREFRONT = "storefront-events" # a heavy, latency-tolerant stream on the same broker (page and product views)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {SIGNALS: 16, REPLIES: 16, STOREFRONT: 32}

# Illustrative catalogue for an illustrative store: names and prices (in cents) are made up.
PRODUCTS = [("Merino wool runner", 9800), ("Wool sock 3-pack", 2200), ("Canvas tote", 3600), ("Linen shirt", 6400),
            ("Stoneware mug", 1800), ("Whole bean coffee 1 kg", 2900), ("Cork yoga mat", 7800), ("Fleece hoodie", 5800),
            ("Soy candle", 2400), ("Mineral sunscreen", 1600), ("Trail cap", 3200), ("Gift card", 5000)]

# The buyer's state the conversation shows, as Shopify's Help Center describes it: cart adds and removes as
# events in the conversation, the cart subtotal and item quantities in the header, and the order once placed.
CART, CHECKOUT, ORDERED = "cart", "checkout", "ordered"
KEEP_EVENTS = 6                  # conversation events kept per buyer (newest last): constant space per buyer


def buyer_id(n: int) -> str:
    return f"B{n:09d}"           # illustrative buyer IDs


def part(buyer: str, partitions: int) -> int:
    # One partition per buyer, the same function in every script, so a buyer's cart and checkout events,
    # which reach Kafka from different sources, are applied in one order on the lane.
    return zlib.crc32(buyer.encode()) % partitions


def money(cents: int) -> str:
    return f"${cents / 100:,.2f}"


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
