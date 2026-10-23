"""Shared settings for the session sequence lane: every product view, item page visit and cart add on Instacart
reaches the shopper's session sequence before that shopper's next ads carousel request, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "instacart-ads"
ACTIONS = "session-actions"      # the lane: product views, item page visits, cart adds, keyed by user
CAROUSELS = "carousel-requests"  # the shopper's next ads carousel, built from the session sequence
FIREHOSE = "event-firehose"      # a heavy, latency-tolerant event stream on the same broker (impressions, page loads)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {ACTIONS: 16, CAROUSELS: 16, FIREHOSE: 32}

# Illustrative catalogue. Retailer types are the kinds Instacart's ads retrieval post names; aisles and the
# product codes below are made up, written in the four-codeword Semantic ID shape the post describes.
RETAILER_TYPES = ["grocery", "big box", "pet", "beauty", "home goods", "alcohol"]
AISLES = ["produce", "meat & seafood", "bakery", "dairy & eggs", "pantry", "condiments", "snacks", "beverages",
          "frozen", "breakfast", "pet food", "pet care", "skin care", "hair care", "laundry", "cleaning",
          "paper goods", "wine", "beer", "spirits", "baby", "health care", "deli", "international"]
KINDS = ["product_view", "item_page_visit", "add_to_cart"]       # the session the post says the model reads

KEEP = 20                        # product tokens kept per session (illustrative sequence length)


def user_id(n: int) -> str:
    return f"u{n:09d}"           # illustrative user IDs


def sid(aisle: int, rnd) -> str:
    """An illustrative Semantic ID: four codewords, the first one shared by an aisle, so similar products share
    a prefix. Instacart's real codebooks are learned (RQ-VAE); these are random."""
    return f"{aisle * 7 + 3}_{rnd.randrange(16)}_{rnd.randrange(256)}_{rnd.randrange(256)}"


def part(user: str, partitions: int) -> int:
    # One partition per user, the same function in every script, so a shopper's actions stay in order.
    return zlib.crc32(user.encode()) % partitions


def fold(state, sess: str, retailer: str, kind: str, token: str):
    """Apply one action to a shopper's session sequence and return it.

    state is [session, retailer type, cart tokens, session tokens]: the parts of the prompt that change
    within a session. A cart add goes into the cart segment; every action goes into the session sequence.
    Both keep the last KEEP tokens, so space is constant per shopper. A new session starts from nothing."""
    if state is None or state[0] != sess:
        state = [sess, retailer, [], []]
    state[1] = retailer
    if kind == "add_to_cart":
        state[2].append(token)
        del state[2][:-KEEP]
    state[3].append(token)
    del state[3][:-KEEP]
    return state


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
