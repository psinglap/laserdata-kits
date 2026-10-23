"""Shared settings for the position-update lane kit: fills reach the Holdings service before the app refreshes, on Apache Iggy."""
import asyncio
import json
import time
import zlib

from apache_iggy import Consumer, PollingStrategy

STREAM = "trading"
POSITIONS = "position-updates"       # the lane: every fill's resulting position, read by the Holdings service
ORDER_UPDATES = "order-updates"      # the order update the app receives; it refreshes Holdings right after
DATAPLATFORM = "dataplatform-events" # a heavy, latency-tolerant neighbour on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"
PARTITIONS = {POSITIONS: 16, ORDER_UPDATES: 16, DATAPLATFORM: 32}

# From Groww's own post: the app refreshes Holdings as soon as the order update reaches it,
# which left "only about 500 ms" for the position update to reach the Holdings service.
REFRESH_MS = 500


def user_id(n: int) -> str:
    return f"U{n:08d}"


def instrument(n: int) -> str:
    return f"NSE-EQ-{n:04d}"           # illustrative instrument IDs


def part(user: str, partitions: int) -> int:
    # One partition per user, the same function in every script, so a user's fills stay in order
    # and the Holdings service sees them in the order OMS executed them.
    return zlib.crc32(user.encode()) % partitions


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


async def read_partitions(client, topic, partitions, name, handle, *, replay=False, commit_s=None, batch=5000, poll_ms=1.0):
    # Direct reads on explicit partitions, not a consumer group: a group visits one partition per poll,
    # which added 10-25 ms of queueing on CI runners. One connection polls each partition in turn.
    # replay=True rebuilds from the first message; otherwise resume from the stored offset, stored on the
    # broker at most once every commit_s seconds (never on every poll).
    consumers = {p: Consumer.Single(f"{name}-p{p}") for p in partitions}
    nxt, committed = {}, {}
    while True:
        more = False
        for p in partitions:
            start = PollingStrategy.Offset(nxt[p]) if p in nxt else PollingStrategy.First() if replay else PollingStrategy.Next()
            commit = commit_s is not None and time.monotonic() - committed.get(p, 0) >= commit_s
            ms = await client.poll_messages(STREAM, topic, consumer=consumers[p], partition_id=p, count=batch,
                                            auto_commit=commit, polling_strategy=start)
            if ms:
                if commit:
                    committed[p] = time.monotonic()
                nxt[p] = ms[-1].offset() + 1
                for m in ms:
                    await handle(m)
                more = more or len(ms) == batch
        if not more:
            await asyncio.sleep(poll_ms / 1000)
