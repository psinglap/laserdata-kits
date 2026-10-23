"""Shared settings for the Apache Iggy source kit for ClickPipes: OpenTelemetry log events on an Iggy topic, inserted
into a ClickHouse otel_logs table exactly once.

The pipe follows the exactly-once rules ClickHouse documents for Kafka ClickPipes: a high-water mark per partition,
the offset range of every block recorded before it is inserted, and a replayed block inserted again with the same
deterministic deduplication token, so ClickHouse drops it if the first insert had landed.
"""
import asyncio
import base64
import json
import time
import urllib.parse
import urllib.request
import zlib
from datetime import timedelta

from apache_iggy import IggyExpiry

STREAM = "telemetry"
LOGS = "otel-logs"          # what collectors write, keyed by source (region/service/pod)
STATE = "pipe-state"        # the pipe's own log: per-partition high-water marks and the block in flight
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
DEFAULT_CH = "http://default:iggy@127.0.0.1:8123"
PARTITIONS = {LOGS: 16, STATE: 1}
# Logs stay in Iggy for two days: a ClickHouse outage, a pipe redeploy or a backfill replays from the log itself.
RETENTION = {LOGS: IggyExpiry.ExpireDuration(timedelta(days=2)), STATE: IggyExpiry.ExpireDuration(timedelta(days=2))}

TABLE = "otel_logs"
# Columns follow the OpenTelemetry ClickHouse exporter's otel_logs table that ClickStack reads (a subset), plus the
# pipe's own: where each row came from in Iggy, so a gap or a duplicate can always be traced to an offset.
DDL = f"""CREATE TABLE IF NOT EXISTS {TABLE} (
  Timestamp DateTime64(9, 'UTC') CODEC(Delta, ZSTD),
  ServiceName LowCardinality(String),
  SeverityText LowCardinality(String),
  Body String CODEC(ZSTD),
  TraceId String,
  ResourceAttributes Map(LowCardinality(String), String),
  Source String,
  Seq UInt64,
  EventNs UInt64,
  IggyPartition UInt32,
  IggyOffset UInt64,
  InsertedAt DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree
ORDER BY (ServiceName, Timestamp)
SETTINGS non_replicated_deduplication_window = 10000"""
# ClickHouse Cloud tables are SharedMergeTree and deduplicate inserts by default; a self-managed MergeTree needs the
# window set, as above. 10,000 blocks is the default window ClickHouse documents for ClickPipes' exactly-once mode.

# Illustrative sources, shaped like a managed database fleet: servers, keepers, proxies, operators.
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-south-1", "ap-southeast-1"]
SERVICES = [("clickhouse-server", 0.55), ("clickhouse-keeper", 0.10), ("ingress-proxy", 0.15),
            ("k8s-operator", 0.05), ("autoscaler", 0.05), ("backup-agent", 0.05), ("billing-usage", 0.05)]
SEVERITY = [("INFO", 0.80), ("DEBUG", 0.12), ("WARN", 0.06), ("ERROR", 0.02)]


def source(i: int) -> tuple[str, str, str]:
    """Source i -> (region, service, pod). Sources are stable, so each keeps its own sequence."""
    cum, r = 0.0, (i * 0.6180339887) % 1.0
    svc = SERVICES[-1][0]
    for name, share in SERVICES:
        cum += share
        if r < cum:
            svc = name
            break
    return REGIONS[i % len(REGIONS)], svc, f"{svc}-{i:05d}"


def part(key: str, partitions: int) -> int:
    # The same function in every script: one source's events stay on one partition, in order.
    return zlib.crc32(key.encode()) % partitions


def iso_ns(ns: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ns // 1_000_000_000)) + f".{ns % 1_000_000_000:09d}"


def event(ns: int, key: str, region: str, svc: str, pod: str, seq: int, sev: str, body: str, trace: str) -> bytes:
    # EventNs goes first, fixed width, so the pipe reads it without parsing the JSON.
    rest = json.dumps({"Timestamp": iso_ns(ns), "ServiceName": svc, "SeverityText": sev, "Body": body, "TraceId": trace,
                       "ResourceAttributes": {"cloud.region": region, "k8s.pod.name": pod}, "Source": key, "Seq": seq},
                      separators=(",", ":"))
    return f'{{"EventNs":{ns:019d},{rest[1:]}'.encode()


def event_ns(payload: bytes) -> int:
    return int(payload[11:30])


class ClickHouse:
    """A small ClickHouse HTTP client (stdlib only). Calls block; run them with asyncio.to_thread."""

    def __init__(self, url: str, database: str = "default", timeout: float = 10):
        u = urllib.parse.urlsplit(url)
        self.base = f"{u.scheme}://{u.hostname}:{u.port or 8123}/"
        self.auth = "Basic " + base64.b64encode(f"{urllib.parse.unquote(u.username or 'default')}:"
                                                 f"{urllib.parse.unquote(u.password or '')}".encode()).decode()
        self.database, self.timeout = database, timeout

    def _post(self, params: dict, body: bytes):
        q = urllib.parse.urlencode({"database": self.database, **params})
        req = urllib.request.Request(self.base + "?" + q, data=body, method="POST",
                                     headers={"Authorization": self.auth})
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return r.read(), r.headers.get("X-ClickHouse-Summary")

    def query(self, sql: str) -> str:
        return self._post({}, sql.encode())[0].decode()

    def insert(self, table: str, rows: bytes, token: str) -> int:
        """Insert one block of JSONEachRow rows; returns the rows ClickHouse wrote (0 when it dropped a repeat)."""
        _, summary = self._post({"query": f"INSERT INTO {table} FORMAT JSONEachRow",
                                 "insert_deduplicate": 1, "insert_deduplication_token": token,
                                 "wait_end_of_query": 1}, rows)
        return int(json.loads(summary or "{}").get("written_rows", -1))


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


async def ends(client, topic) -> dict:
    """Next offset to be written, per partition (0 for an empty partition)."""
    t = await client.get_topic(STREAM, topic)
    return {i: (p.current_offset + 1 if p.messages_count else 0)
            for i, p in enumerate(sorted(t.partitions, key=lambda x: x.id))}


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
