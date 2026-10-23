"""Shared settings for the in-state wallet lane: every account and wallet event a state's sportsbook accepts,
keyed by account, written at the site taking bets and mirrored to the second site in the same state, on Apache Iggy.

A message is one event on one customer account, as the account and wallet service would record it: the account,
its sequence number (one more on every event of that account), what happened (deposit, bet placed, bet settled,
cash out, withdrawal), the bet and the amount, and the account's resulting balance and open bets after the event.
It carries the resulting state, never a delta, so the second site can take over from the newest event alone.
It also carries the state it was accepted in: the mirror refuses to copy an event to a site in any other state.

Accounts, amounts and site names are illustrative.
"""
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STATE = "NJ"                         # one lane per state; events never leave the state they were accepted in
SITES = {"nj-1": "NJ", "nj-2": "NJ"}  # site -> state; the mirror checks both ends against this table
PRIMARY, SECOND = "nj-1", "nj-2"
STREAM = "fd-nj"
LANE = "account-wallet"              # every account and wallet event, keyed by account
STATUS = "mirror-status"             # at nj-2: one line each time the mirror (re)starts, with what it found waiting
PARTITIONS = 8                       # an account always lands on one partition, so its events stay in order
RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=72))   # a weekend of events, room for a long DR drill
PRIMARY_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
SECOND_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8091?nodelay=true"

KINDS = ("bet.placed", "bet.settled", "cash.out", "deposit", "withdrawal")


def partition_of(account: str) -> int:
    # The same function everywhere, so an account's events always land on one partition, in order.
    return zlib.crc32(account.encode()) % PARTITIONS


def account_ids(n: int) -> list:
    return [f"{STATE.lower()}-{i:07d}" for i in range(n)]


def pack(account: str, seq: int, kind: str, bet: str, amount: int, balance: int, open_bets: int, site: str,
         state: str = STATE, waited_ns: int = 0) -> bytes:
    # Accepted time = now, minus how long the event already waited for its write. Amounts are in cents.
    return json.dumps({"a": account, "s": seq, "t": kind, "b": bet, "amt": amount, "bal": balance, "o": open_bets,
                       "st": state, "site": site, "ns": time.time_ns() - waited_ns}, separators=(",", ":")).encode()


def unpack(b: bytes) -> dict:
    return json.loads(b)


def state_of(payload: bytes) -> str:
    # Cheap check for the mirror: the state field, without parsing the whole event.
    i = payload.find(b'"st":"')
    return payload[i + 6:i + 8].decode() if i >= 0 else "?"


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topic(c: IggyClient, topic: str, partitions: int):
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    if await c.get_topic(STREAM, topic) is None:
        try:
            await c.create_topic(STREAM, topic, partitions, message_expiry=RETENTION)
        except RuntimeError:
            pass


async def heads(c: IggyClient, topic: str = LANE) -> list:
    # Next offset per partition (how many events each partition holds), in partition order.
    t = await c.get_topic(STREAM, topic)
    if t is None:
        return [0] * PARTITIONS
    ps = sorted(t.partitions, key=lambda p: p.id)
    return [p.current_offset + 1 if p.messages_count else 0 for p in ps]


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
