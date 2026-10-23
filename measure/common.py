"""Shared settings for the crash spike lane kit: every crash and ANR the ingest worker groups into an issue, keyed by
app, reaches the spike check in milliseconds, so an issue that crosses the app's thresholds is alerted on Slack and
email within a second, instead of at the next hourly run. On Apache Iggy, the broker Measure's self-host stack
already runs.

Names follow Measure's own: the stream is `measure` (bus.DefaultStreamName), `ingest-batch` is the topic the
ingest service already publishes to (9 partitions in self-host/compose.yml), and the spike rule is the one in
backend/alerts: an issue alerts when it occurs at least 100 times in an hour and those occurrences amount to at
least 0.5% of that hour's sessions; then it stays quiet for 7 days. Both thresholds are per app.
"""
import asyncio
import gc
import json
import time
import uuid
import zlib

STREAM = "measure"                  # bus.DefaultStreamName
ISSUES = "issue-occurrences"        # the lane: one record per ingest batch after grouping, keyed by app ID
ALERTS = "spike-alerts"             # the spike check's output, read by the Slack and email senders
INGEST = "ingest-batch"             # the ingest service's existing topic: full batches, the neighbour
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {ISSUES: 9, ALERTS: 1, INGEST: 9}

# Measure's defaults (backend/alerts/alerts/alerts.go): per app, changeable on the Apps page.
MIN_COUNT = 100                     # errorSpikeMinCountThreshold: occurrences of one issue in the window
MIN_RATE = 0.5                      # errorSpikeMinRateThreshold: those occurrences as % of the window's sessions
WINDOW_S = 3600                     # errorSpikeTimePeriod: one hour
COOLDOWN_S = 7 * 24 * 3600          # errorAlertCooldownPeriod: one week per issue
KINDS = {"crash": "Crash Spike", "anr": "ANR Spike"}

NS = uuid.UUID("6d656173-7572-652d-7368-2d6b69742d31")


def app_id(i: int) -> str:
    """A stable UUID per app, as Measure's app IDs are UUIDs."""
    return str(uuid.uuid5(NS, f"app-{i}"))


def part(key: str, partitions: int) -> int:
    # The same function in every script, so all of one app's records reach one reader, in order.
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
                await client.create_topic(STREAM, t, PARTITIONS[t])
            except RuntimeError:
                pass
    while not all([await client.get_topic(STREAM, t) for t in topics]):
        await asyncio.sleep(0.5)


class SpikeWindow:
    """The spike rule over a sliding window, for one app: distinct sessions seen in the window, and occurrences per
    issue (kind, fingerprint). Time is the record's own clock (ns), so the same records give the same answer
    wherever they are applied. Used by the live spike check (on every record) and, separately, by the hourly
    check the kit compares it with (at the top of each hour)."""

    def __init__(self, window_s: float):
        self.w = int(window_s * 1e9)
        self.sessions = {}                          # session -> last seen ns, oldest first
        self.occ = {}                               # (kind, fingerprint) -> [(ns, n)], oldest first
        self.count = {}                             # (kind, fingerprint) -> occurrences in the window

    def add(self, ns: int, session: str, issues):
        self.sessions.pop(session, None)            # re-insert at the end: the dict stays ordered by last seen
        self.sessions[session] = ns
        for kind, fp, n in issues:
            k = (kind, fp)
            self.occ.setdefault(k, []).append((ns, n))
            self.count[k] = self.count.get(k, 0) + n

    def expire(self, now_ns: int, keys=None):
        """Drop what fell out of the window: every session, and the occurrences of the given issues (all issues
        when keys is None; a full sweep once a second keeps memory bounded, the hot path touches only its own)."""
        cut = now_ns - self.w
        while self.sessions:
            s, t = next(iter(self.sessions.items()))
            if t > cut:
                break
            del self.sessions[s]
        for k in (list(self.occ) if keys is None else keys):
            q = self.occ.get(k)
            if q is None:
                continue
            i = 0
            while i < len(q) and q[i][0] <= cut:
                self.count[k] -= q[i][1]
                i += 1
            if i:
                del q[:i]
            if not q:
                del self.occ[k], self.count[k]

    def spiking(self, k, min_count: int, min_rate: float):
        """(count, sessions, rate %) if issue k meets both thresholds now, else None."""
        c = self.count.get(k, 0)
        if c < min_count:
            return None
        s = len(self.sessions)
        rate = c * 100 / s if s else 0.0
        return (c, s, rate) if rate >= min_rate else None


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
