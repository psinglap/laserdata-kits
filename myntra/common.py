"""Shared settings for the session context lane: every shopper action on the Myntra app reaches the shopper's
session context before that shopper's next type-ahead request, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "myntra-nrt"
ACTIONS = "shopper-actions"      # the lane: searches, product views, wishlist and bag adds, filters, keyed by user
TYPEAHEAD = "typeahead-requests" # the shopper's next type-ahead request, boosted from that session context
CLICKS = "clickstream"           # a heavy, latency-tolerant stream on the same broker (page loads, impressions)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {ACTIONS: 16, TYPEAHEAD: 16, CLICKS: 32}

# Illustrative catalogue: real Myntra categories and Myntra's own labels; IDs and mixes are made up.
CATEGORIES = ["tshirts", "shirts", "jeans", "kurtas", "kurta sets", "sarees", "dresses", "tops", "sneakers",
              "casual shoes", "sports shoes", "heels", "flats", "watches", "handbags", "backpacks", "sunglasses",
              "jackets", "sweatshirts", "track pants", "shorts", "lehenga cholis", "trousers", "lipstick",
              "perfumes", "cushion covers", "bedsheets", "earrings", "wallets", "ethnic jackets"]
BRANDS = ["Roadster", "HRX", "Mast & Harbour", "Dressberry", "Anouk", "HERE&NOW", "Kook N Keech", "Moda Rapido"]
GENDERS = ["men", "women"]
# How strongly each action says "this is what I want now" (a search or a bag add more than a glance).
ACTION_WEIGHT = {"search": 1.0, "view": 0.6, "wishlist": 0.8, "add_to_bag": 1.0, "filter": 0.8}

HALF_LIFE_S = 60.0               # short-term context decays: an annotation loses half its weight a minute (illustrative)
MIN_WEIGHT = 0.05                # below this, an annotation has aged out
KEEP = 8                         # annotations kept per shopper: constant space per shopper


def user_id(n: int) -> str:
    return f"U{n:09d}"           # illustrative user IDs


def part(user: str, partitions: int) -> int:
    # One partition per user, the same function in every script, so a shopper's actions stay in order.
    return zlib.crc32(user.encode()) % partitions


def fold(ctx, sess: str, ann, ns: int):
    """Apply one action to a shopper's session context and return the new context.

    ctx is [session, last action ns, {"field=value": weight}]. Weights decay with the action's own time,
    so the app and the context service, folding the same actions in the same order, hold the same context.
    A new session starts from nothing: the context lives while the session does."""
    if ctx is None or ctx[0] != sess:
        ctx = [sess, ns, {}]
    w = ctx[2]
    if w and ns > ctx[1]:
        k = 0.5 ** ((ns - ctx[1]) / 1e9 / HALF_LIFE_S)
        for a in w:
            w[a] *= k
    for field, value, weight in ann:
        a = f"{field}={value}"
        w[a] = w.get(a, 0.0) + weight
    if len(w) > KEEP or min(w.values()) < MIN_WEIGHT:
        ctx[2] = w = dict(sorted(((a, x) for a, x in w.items() if x >= MIN_WEIGHT), key=lambda i: -i[1])[:KEEP])
    ctx[1] = max(ctx[1], ns)
    return ctx


def top(ctx, field="category") -> str:
    """The value of the strongest annotation of one field: what the type-ahead boosts first."""
    if not ctx:
        return ""
    best, bw = "", 0.0
    for a, x in ctx[2].items():
        f, _, v = a.partition("=")
        if f == field and x > bw:
            best, bw = v, x
    return best


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
