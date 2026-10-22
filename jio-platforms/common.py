"""Shared settings for the 5G core probe record lane kit: vProbe records in, network-function alerts out, on Apache Iggy.

Jio's Cloud Native 5G Core whitepaper (2021) describes vProbes embedded in the network functions that
collect Streaming Data Records (SDRs) from 4G/5G network nodes, carried over a real-time conductor fabric
into ATOM, which detects anomalous network patterns down to the individual end node. This kit runs that
one lane: SDRs in, keyed by network-function instance; each instance's resulting health state out to the
workflow engine.
"""
import asyncio
import json
import time
import zlib

STREAM = "atom-network"
SDRS = "vprobe-sdr"          # the lane in: one Streaming Data Record per procedure, keyed by NF instance
ALERTS = "nf-health"         # the lane out: each NF instance's resulting health state, read by the workflow engine
RAN = "ran-counters"         # a heavy, latency-tolerant neighbour: RAN counters and logs for correlation
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"
PARTITIONS = {SDRS: 16, ALERTS: 4, RAN: 16}

# Circles, NF types and instance counts are illustrative. Each pool (one NF type in one circle) has a few
# instances; the whitepaper's Data Management Services layer keeps session data outside the CNF, so a peer
# instance in the same pool can take new procedures if one is drained.
CIRCLES = [("mu", 0.16), ("mh", 0.14), ("dl", 0.13), ("ka", 0.11), ("tn", 0.10), ("upe", 0.10),
           ("gj", 0.09), ("wb", 0.08), ("ap", 0.05), ("kl", 0.04)]
NF_TYPES = [("amf", 0.45), ("smf", 0.35), ("upf", 0.20)]    # share of procedures each NF type records
INSTANCES = 3                                                # per pool
PROCEDURES = {"amf": ["registration", "service-request", "handover"],
              "smf": ["pdu-session-establish", "pdu-session-modify"],
              "upf": ["n4-session-establish", "n4-session-modify"]}
CAUSE = {"amf": "5GMM #22 congestion", "smf": "5GSM #26 insufficient resources", "upf": "PFCP #74 no resources"}


def pools():
    """(pool, share of procedures): every NF type in every circle, 30 pools."""
    return [(f"{t}-{c}", tw * cw) for t, tw in NF_TYPES for c, cw in CIRCLES]


def nfs():
    """(NF instance, share of SDRs): every instance of every pool, 90 keys."""
    return [(f"{p}-{i:02d}", w / INSTANCES) for p, w in pools() for i in range(1, INSTANCES + 1)]


def pool_of(nf: str) -> str:
    return nf.rsplit("-", 1)[0]


def part(key: str, partitions: int) -> int:
    # One partition per key, the same function in every script, so an instance's records stay in order.
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


async def read_partitions(client, topic, pids, name, on_message, batch=1000, poll_ms=1.0):
    """Read the given partitions directly, each in turn, from the start of the log, then from our own position.

    No consumer group: a group reader visits one partition per poll, so a record waits for its partition's
    turn. Nothing is stored on the broker; a restarted reader replays the log and rebuilds its state.
    A full batch means more is waiting, so no pause.
    """
    from apache_iggy import Consumer, PollingStrategy
    who = Consumer.Single(name)
    nxt = {}
    while True:
        more = False
        for p in pids:
            ms = await client.poll_messages(STREAM, topic, consumer=who, partition_id=p, count=batch, auto_commit=False,
                                            polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                            else PollingStrategy.First())
            if ms:
                nxt[p] = ms[-1].offset() + 1
                for m in ms:
                    on_message(m)
                more = more or len(ms) == batch
        if not more:
            await asyncio.sleep(poll_ms / 1000)


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
