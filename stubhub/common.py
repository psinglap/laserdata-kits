"""Shared settings for the StubHub listing lane kit: every seller listing change (tickets left and price) reaches
the event pages and checkout in milliseconds, on Apache Iggy."""
import asyncio
import json
import time
import zlib

from apache_iggy import Consumer, PollingStrategy

STREAM = "marketplace"
LISTINGS = "listing-changes"     # the lane: one record per listing change, keyed by event:listing, resulting state
ORDERS = "orders"                # buyers' seat picks, back to the listing service
STATUS = "order-status"          # confirmed, or rejected because the tickets were already sold
CLICKS = "clickstream"           # a heavy, latency-tolerant stream on the same broker (page views, search logs)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = 16


def key(event: int, listing: int) -> str:
    return f"E{event:05d}:L{listing:07d}"


def part_of(k: str, partitions: int = PARTITIONS) -> int:
    # The one partition function used everywhere: a listing's changes always land in one partition, in order.
    return zlib.crc32(k.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


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
