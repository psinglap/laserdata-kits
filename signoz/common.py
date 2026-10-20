"""Shared settings for the ingest buffer kit: OTLP log batches on Apache Iggy, drained into ClickHouse."""
import json
import time
import zlib

STREAM = "signoz-ingest"
LOGS = "otlp_logs"                     # the topic name the Kafka exporter in signoz-otel-collector defaults to
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: no 40 ms delayed-ACK stalls
CH = dict(host="127.0.0.1", port=8123, username="lane", password="lane")
TABLE = "signoz_lane.logs"


def partition(tenant: str, partitions: int) -> int:
    return zlib.crc32(tenant.encode()) % partitions     # every batch of one tenant lands in one partition


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


class Window:
    def __init__(self):
        self.samples = []

    def add(self, v, n=1):
        self.samples.extend([v] * n)

    def flush(self):
        s = sorted(self.samples) or [0.0]
        pick = lambda q: s[min(len(s) - 1, int(q * len(s)))]
        out = {"n": len(self.samples), "p50": pick(0.5), "p99": pick(0.99), "p999": pick(0.999), "max": s[-1]}
        self.samples = []
        return out
