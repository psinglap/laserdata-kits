"""Shared settings for the last-searched lane: every hotel search and hotel view on MakeMyTrip, from the B2C app
and from corporate (B2B) travel, reaches that traveller's last-searched list before their next tap on the search
bar, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "mmt-hotels"
SEARCHES = "hotel-searches"     # the lane: hotel searches and hotel views from B2C and B2B, keyed by user
TAPS = "searchbar-taps"         # the traveller's next tap on the search bar, served the last-searched list
CLICKS = "clickstream"          # a heavy, latency-tolerant stream on the same broker (page loads, impressions)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
# 64 partitions on the lane, as on the Kafka stream behind "last-searched" hotels (Databricks blog, Apr 2026).
PARTITIONS = {SEARCHES: 64, TAPS: 16, CLICKS: 32}
LAST_N = 10                      # hotels kept per traveller: the list shown under the search bar (illustrative)

# Illustrative inventory: real Indian destinations; hotel IDs are made up.
CITIES = ["Goa", "Jaipur", "Udaipur", "Manali", "Shimla", "Mumbai", "Bengaluru", "New Delhi", "Hyderabad",
          "Chennai", "Kochi", "Munnar", "Rishikesh", "Mussoorie", "Ooty", "Pondicherry", "Varanasi", "Agra",
          "Darjeeling", "Gangtok", "Coorg", "Lonavala", "Jaisalmer", "Pune", "Kolkata", "Amritsar", "Nainital",
          "Mahabaleshwar", "Alleppey", "Srinagar"]
HOTELS_PER_CITY = 400


def user_id(n: int, seg: str) -> str:
    return f"{'B' if seg == 'b2b' else 'C'}{n:09d}"    # illustrative user IDs, B2C and B2B


def hotel(city: str, n: int) -> str:
    return f"{city[:3].upper()}{n:05d}"                 # illustrative hotel IDs


def part(user: str, partitions: int) -> int:
    # One partition per user, the same function in every script, so a traveller's searches stay in order.
    return zlib.crc32(user.encode()) % partitions


def fold(recent, h: str, city: str, checkin: str):
    """Apply one search or view to a traveller's last-searched list and return the new list.

    The list is [[hotel, city, check-in], ...], newest first, at most LAST_N long. Searching a hotel again
    moves it to the front. The app and the last-searched service fold the same events in the same order, so
    they hold the same list."""
    out = [[h, city, checkin]]
    for e in recent or ():
        if e[0] != h:
            out.append(e)
            if len(out) == LAST_N:
                break
    return out


def head(recent) -> str:
    """The hotel at the top of the list: the one the traveller searched last."""
    return recent[0][0] if recent else ""


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
