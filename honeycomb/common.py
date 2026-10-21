"""Shared settings for the ingest freshness lane kit: a receipt for every batch the ingest service has
enqueued, keyed by dataset, and each Retriever's progress per Kafka partition, into the freshness service
that heartbeat triggers ask before they page, in milliseconds, on Apache Iggy."""
import asyncio
import gc
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyExpiry

STREAM = "ingest-freshness"
LANE = "freshness-lane"          # the lane: a receipt per enqueued batch (keyed by dataset) and each Retriever
                                  # node's applied offset per Kafka partition (keyed by that partition)
VERDICTS = "heartbeat-verdicts"   # the freshness service's verdict changes (read by the truth check)
BACKFILL = "backfill-events"      # raw event batches replayed after a cutover: big, read by nobody here (the neighbour)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {LANE: 16, VERDICTS: 1, BACKFILL: 16}
RETENTION = {LANE: IggyExpiry.ExpireDuration(timedelta(minutes=15)),   # enough to rebuild every dataset's state
             VERDICTS: IggyExpiry.ExpireDuration(timedelta(hours=1)),
             BACKFILL: IggyExpiry.ExpireDuration(timedelta(hours=1))}
KAFKA_PARTITIONS = 100            # "~100 partitions" on the main events topic (Honeycomb, SREcon Americas 2023)


def dataset(i: int) -> str:
    # An illustrative team/environment/dataset path; yours come from the ingest service.
    return f"team-{i // 12:05d}/{('production', 'staging', 'dev')[i % 3]}/svc-{i % 12:02d}"


def progress_key(kp: int) -> str:
    return f"kafka-partition-{kp}"   # progress for one Kafka partition stays in order on one lane partition


def part(key: str, partitions: int) -> int:
    # One Iggy partition per key, the same function in every script, so a dataset's receipts stay in order.
    return zlib.crc32(key.encode()) % partitions


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
                await client.create_topic(STREAM, t, PARTITIONS[t], message_expiry=RETENTION[t])
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


class Stalls:
    """Longest garbage-collection pause and longest event-loop stall since the last read, in ms. Printed next to
    the latency, so a slow second can be traced to this process (or ruled out) rather than guessed at."""

    def __init__(self):
        self.gc = self.loop = 0.0
        self._t = 0.0
        gc.callbacks.append(self._cb)

    def _cb(self, phase, info):
        if phase == "start":
            self._t = time.perf_counter()
        else:
            self.gc = max(self.gc, (time.perf_counter() - self._t) * 1000)

    async def watch(self):
        while True:
            t = time.perf_counter()
            await asyncio.sleep(0.005)
            self.loop = max(self.loop, (time.perf_counter() - t - 0.005) * 1000)

    def read(self):
        out = f"stall gc {self.gc:.0f}ms loop {self.loop:.0f}ms"
        self.gc = self.loop = 0.0
        return out
