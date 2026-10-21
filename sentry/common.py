"""Shared settings for the Crons check-in lane kit: every cron check-in and every clock pulse, keyed by
monitor, into the monitor consumer and its clock, on Apache Iggy.

The clock works the way Sentry Crons describes it: each partition's clock is the log time of the newest
message read from it, and the clock ticks a minute only when every partition has passed that minute. A tick
marks missed check-ins and timed-out jobs. So the clock moves at the pace of the slowest partition, and the
lane's tail latency is how late every missed-job alert is.
"""
import asyncio
import gc
import json
import time
import zlib
from datetime import timedelta

from apache_iggy import IggyExpiry

STREAM = "sentry-crons"
CHECKINS = "ingest-monitors"      # check-ins (in_progress, ok, error) keyed by monitor, and clock pulses
VERDICTS = "monitor-verdicts"     # missed and timed-out marks from the clock (read by the truth check)
EVENTS = "ingest-events"          # error events from a customer's spike: big, read by nobody here (the neighbour)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {CHECKINS: 16, VERDICTS: 1, EVENTS: 16}
RETENTION = {CHECKINS: IggyExpiry.ExpireDuration(timedelta(hours=1)),
             VERDICTS: IggyExpiry.ExpireDuration(timedelta(hours=1)),
             EVENTS: IggyExpiry.ExpireDuration(timedelta(hours=1))}
SENTRY_PARTITIONS = 128           # partitions on Sentry's check-in topic (getsentry/sentry issue #55821, 2023)


def monitor(i: int) -> str:
    # An illustrative org/project/monitor slug; yours come from the check-in's monitor_slug.
    return f"org-{i // 25:05d}/proj-{(i // 5) % 5}/{('nightly-backup', 'sync-invoices', 'send-digest', 'rotate-keys', 'reindex')[i % 5]}-{i:06d}"


def part(key: str, partitions: int) -> int:
    # One partition per monitor, the same function in every script, so a monitor's check-ins stay in order.
    return zlib.crc32(key.encode()) % partitions


def every_minutes(schedule: str) -> int:
    """Minutes between runs for the crontab shapes the kit uses: '*/k * * * *', '* * * * *', '0 * * * *'."""
    m = schedule.split()[0]
    if m == "*":
        return 1
    if m.startswith("*/"):
        return int(m[2:])
    return 60


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


def log_seconds(ts: int) -> float:
    """The broker's append time of a message, in seconds. Iggy stamps microseconds; accept ms or ns too."""
    if ts > 1e17:
        return ts / 1e9
    if ts > 1e14:
        return ts / 1e6
    return ts / 1e3


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
