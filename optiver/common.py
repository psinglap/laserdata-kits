"""Shared settings for the table change log kit: config and pricing table changes, on Apache Iggy.

Every committed row change to a subscribed table is written once to the `table-changes` topic, one
partition per group of tables (a table always lands on the same partition, so its changes stay in
commit order). A change carries the resulting row, never a delta, the table's own sequence number, the
commit LSN, the commit time, and the table's state hash after the change, so any reader can check that
its copy of the table matches the database's, row for row.

`checkpoint.py` writes the full state of each partition's tables to `table-checkpoints` every few
seconds, with the change offset it covers. A process that starts or restarts loads the latest
checkpoint for its tables from the log and replays the changes after it: no snapshot query on Postgres.
"""
import asyncio
import json
import zlib
from datetime import timedelta

from apache_iggy import IggyClient, IggyExpiry

STREAM = "pg-tables"
CHANGES = "table-changes"
CHECKPOINTS = "table-checkpoints"
CHANGE_RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=24))      # a trading day of changes, for replay
CHECKPOINT_RETENTION = IggyExpiry.ExpireDuration(timedelta(hours=1))   # only the latest one is ever read
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"

# Illustrative table names: "relatively small tables that change very frequently, like configuration and
# pricing data" (Optiver Technology blog, "Pushing Postgres beyond storage", Apr 2026). Use your own.
BASES = ["pricing.underlying_params", "pricing.vol_params", "pricing.rate_curves", "pricing.dividend_inputs",
         "config.instrument_settings", "config.strategy_params", "config.quote_widths", "config.risk_limits"]
REGIONS = ["ams", "chi", "syd", "shg", "lon", "hkg"]


def tables(n: int) -> list:
    return [f"{BASES[i % len(BASES)]}_{REGIONS[(i // len(BASES)) % len(REGIONS)]}" for i in range(n)]


def partition_of(table: str, partitions: int) -> int:
    # The same function everywhere, so a table's changes always land on one partition, in commit order.
    return zlib.crc32(table.encode()) % partitions


def row_hash(key: str, row: str) -> int:
    return zlib.crc32(f"{key}|{row}".encode())


def encode(table, key, op, row, tseq, lsn, ns, h) -> bytes:
    return json.dumps({"t": table, "k": key, "o": op, "r": row, "s": tseq, "l": lsn, "n": ns, "h": h},
                      separators=(",", ":")).encode()


class Table:
    """One subscriber's copy of one table: rows, the last applied sequence, and the running state hash.

    The hash is the sum of crc32(key|row) over all rows, mod 2**32, kept up to date on every change.
    The writer computes the same hash from its own rows and puts it on every change it sends, so
    `apply()` can say whether this copy still matches the database after each change.
    """
    __slots__ = ("rows", "seq", "h")

    def __init__(self):
        self.rows, self.seq, self.h = {}, 0, 0

    def apply(self, m: dict) -> str:
        """Returns 'repeat', 'gap+ok', 'ok', 'gap+bad' or 'bad' ('bad': the copy no longer matches)."""
        s = m["s"]
        if s <= self.seq:
            return "repeat"                     # already applied (an overlap after a checkpoint, or a redelivery)
        gap = "gap+" if s > self.seq + 1 else ""
        k, old = m["k"], self.rows.get(m["k"])
        if old is not None:
            self.h = (self.h - row_hash(k, old)) & 0xFFFFFFFF
        if m["o"] == "D":
            self.rows.pop(k, None)
        else:
            self.rows[k] = m["r"]
            self.h = (self.h + row_hash(k, m["r"])) & 0xFFFFFFFF
        self.seq = s
        return gap + ("ok" if self.h == m["h"] else "bad")

    def load(self, cp: dict):
        self.rows = dict(cp["rows"])
        self.seq, self.h = cp["s"], cp["h"]


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topics(c: IggyClient, partitions: int):
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                                # created by another process in the meantime
    for topic, keep in ((CHANGES, CHANGE_RETENTION), (CHECKPOINTS, CHECKPOINT_RETENTION)):
        if await c.get_topic(STREAM, topic) is None:
            try:
                await c.create_topic(STREAM, topic, partitions, message_expiry=keep)
            except RuntimeError:
                pass


async def wait_for_topics(c: IggyClient, who: str) -> int:
    while not (await c.get_topic(STREAM, CHANGES) and await c.get_topic(STREAM, CHECKPOINTS)):
        print(f"[{who}] waiting for pg_writer.py to create the topics…", flush=True)
        await asyncio.sleep(1)
    return (await c.get_topic(STREAM, CHANGES)).partitions_count


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
