"""Shared settings for the decision outcome lane: every delivered decision and every goal event reaches the
online learner of CleverTap's Tesseract Decisioning Engine before that user's next decision, on Apache Iggy."""
import asyncio
import json
import random
import time
import zlib

STREAM = "clevertap-decisioning"
OUTCOMES = "decision-outcomes"     # the lane: delivered decisions and goal events, keyed by user
REQUESTS = "decision-requests"     # the next decision request for a user (an app open, a journey step)
HISTORY = "event-history"          # a heavy, latency-tolerant stream on the same broker (raw user actions)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {OUTCOMES: 16, REQUESTS: 16, HISTORY: 32}

# Goal events as the Decisioning Engine post names them: a marketer names positive signals (checkout,
# add-to-cart) and negative signals (uninstall, subscription cancellation). "delivered" is the logged decision.
GOALS = ["add_to_cart", "checkout", "uninstall", "subscription_cancel"]
KINDS = ["delivered"] + GOALS
REWARD = {"add_to_cart": 1, "checkout": 1, "uninstall": -1, "subscription_cancel": -1}

# Each action is a campaign with its delivery choices (channel, timing, offer), or no action at all.
# Illustrative campaigns from the post's recurring themes: cart reminders, festive discounts.
ACTIONS = ["cart_reminder_push", "festive_discount_inapp", "cross_sell_email", "winback_offer_sms", "no_action"]
CAP = 3                            # frequency cap: touches per user per day (illustrative; a marketer guardrail)
HORIZON_S = 3600                   # goal horizon H: goal events this long after a decision count as its reward


def user_id(n: int) -> str:
    return f"ID{n:09d}"            # illustrative identities


def part(user: str, partitions: int) -> int:
    # One partition per user, the same function in every script, so a user's outcomes stay in order.
    return zlib.crc32(user.encode()) % partitions


def scores(user: str) -> dict:
    """Stands in for s(x, a), the score both learners give each action for this user. Here it is a fixed
    function of the user, so every script ranks the same way; the kit tests the lane, not the model."""
    r = random.Random(zlib.crc32(user.encode()))
    s = {a: r.random() for a in ACTIONS[:-1]}
    s["no_action"] = 0.05
    return s


def fold(st, kind: str, action: str = "", ns: int = 0):
    """Apply one record to a user's state and return the new state. The app side and the learner fold the
    same records in the same order, so they agree on what the next decision may be.

    st is [cart open, checked out, uninstalled, cancelled, touches today, last action, last decision ns]."""
    if st is None:
        st = [False, False, False, False, 0, "", 0]
    if kind == "delivered":
        if action != "no_action":
            st[4] += 1
        st[5], st[6] = action, ns
    elif kind == "add_to_cart":
        st[0] = True
    elif kind == "checkout":
        st[0], st[1] = False, True
    elif kind == "uninstall":
        st[2] = True
    elif kind == "subscription_cancel":
        st[3] = True
    return st


def feasible(st) -> list:
    """Guardrails as the support of the policy: only actions allowed for this user right now."""
    if st is None:
        st = fold(None, "")
    if st[4] >= CAP:
        return ["no_action"]                 # frequency cap reached: nothing else is allowed today
    ok = ["no_action", "cross_sell_email"] if st[1] else ["no_action"]
    if st[3]:
        ok.append("winback_offer_sms")
    if not st[2]:                            # in-app and push need the app on the phone
        ok.append("festive_discount_inapp")
        if st[0]:
            ok.append("cart_reminder_push")  # a cart reminder only while the cart is open
    return ok


def decide(user: str, st) -> str:
    """The next decision: the highest-scored action among the feasible ones."""
    s = scores(user)
    return max(feasible(st), key=s.__getitem__)


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
