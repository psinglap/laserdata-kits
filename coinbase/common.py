"""Shared settings for the Coinbase chain event lane kit: every BLOCK_ADDED and BLOCK_REMOVED event that
ChainStorage records, per chain, in sequence order, to every indexer in milliseconds, on Apache Iggy.

An event on the lane has the shape of ChainStorage's BlockchainEvent (coinbase/chainstorage, api.proto):
chain, sequence_num (monotonically increasing per chain), type (BLOCK_ADDED or BLOCK_REMOVED) and the block
identifier (tag, hash, height, skipped, timestamp), plus the time ChainStorage recorded it, in ns.

Chain settings (block time, StreamChainEvents interval, irreversible distance) are copied from the per-chain
config files in the open-source ChainStorage repo. Reorg rates are illustrative, not measured.
"""
import asyncio
import hashlib
import json
import random
import time

from apache_iggy import IggyClient

STREAM = "chainstorage"
EVENTS = "chain-events"          # the lane: one record per chain event, ordered per chain
PARTITIONS = 4                   # each chain lives in exactly one partition, so its events stay in order
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once

# name: (block time s, StreamChainEvents streaming_interval s, irreversible_distance, reorgs per block, max depth)
CHAINS = {
    "ethereum-mainnet": (12.0, 1.0, 12, 0.004, 1),
    "base-mainnet": (2.0, 1.0, 10, 0.0005, 1),
    "polygon-mainnet": (2.0, 1.0, 400, 0.01, 3),
    "solana-mainnet": (0.4, 0.25, 1, 0.001, 1),
    "arbitrum-mainnet": (0.3, 1.0, 1, 0.0002, 1),
    "optimism-mainnet": (2.0, 1.0, 50, 0.0005, 1),
    "bsc-mainnet": (3.0, 1.0, 20, 0.004, 2),
    "avacchain-mainnet": (2.0, 1.0, 1, 0.001, 1),
    "aptos-mainnet": (0.4, 1.0, 1, 0.0, 1),
    "fantom-mainnet": (1.0, 1.0, 1, 0.001, 1),
    "dogecoin-mainnet": (60.0, 1.0, 60, 0.02, 1),
    "bitcoin-mainnet": (600.0, 1.0, 2, 0.01, 1),
}
CHAIN_IDS = {c: i for i, c in enumerate(CHAINS)}
START_HEIGHT = {"ethereum-mainnet": 23_612_000, "base-mainnet": 37_104_000, "polygon-mainnet": 78_220_000,
                "solana-mainnet": 374_900_000, "arbitrum-mainnet": 391_400_000, "optimism-mainnet": 142_700_000,
                "bsc-mainnet": 64_300_000, "avacchain-mainnet": 70_100_000, "aptos-mainnet": 430_000_000,
                "fantom-mainnet": 112_000_000, "dogecoin-mainnet": 5_940_000, "bitcoin-mainnet": 919_000}


def part_of(chain: str) -> int:
    # The one partition function used everywhere: a chain's events always land in one partition, in order.
    return CHAIN_IDS[chain] % PARTITIONS


def block_hash(chain: str, height: int, fork: int) -> str:
    return "0x" + hashlib.blake2b(f"{chain}:{height}:{fork}".encode(), digest_size=32).hexdigest()


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def connect(url: str) -> IggyClient:
    # One connection per writer and per reader, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def ensure_topics(c: IggyClient):
    """Create the stream and topic if they are missing, so any script can start first."""
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    if await c.get_topic(STREAM, EVENTS) is None:
        try:
            await c.create_topic(STREAM, EVENTS, PARTITIONS)
        except RuntimeError:
            pass
    while not await c.get_topic(STREAM, EVENTS):
        await asyncio.sleep(0.5)


class Canon:
    """An indexer's view of one chain: the canonical blocks near the tip, rebuilt from + and - events.

    ChainStorage's rule: given +1, +2, +3, -3, -2, +2', +3', the canonical chain is +1, +2', +3'. A removal must
    remove the current tip; an addition must extend it by one height. Anything else is a broken chain."""
    __slots__ = ("seq", "blocks", "breaks", "gaps", "dups")

    def __init__(self):
        self.seq, self.blocks, self.breaks, self.gaps, self.dups = 0, [], 0, 0, 0

    def tip(self):
        return self.blocks[-1] if self.blocks else None

    def apply(self, e: dict) -> bool:
        s = e["seq"]
        if s <= self.seq:
            self.dups += 1                # at-least-once redelivery: already applied
            return False
        if self.seq and s != self.seq + 1:
            self.gaps += 1                # an event this indexer never saw
        self.seq = s
        if e["type"] == "BLOCK_ADDED":
            if self.blocks and e["height"] != self.blocks[-1][0] + 1:
                self.breaks += 1
            self.blocks.append((e["height"], e["hash"]))
            if len(self.blocks) > 512:
                del self.blocks[:256]     # keep the reorg window, not the whole chain
        else:
            if not self.blocks or self.blocks[-1][1] != e["hash"]:
                self.breaks += 1
            else:
                self.blocks.pop()
        return True


class PollModel:
    """The published StreamChainEvents loop, as a model to compare against (not a measurement).

    From coinbase/chainstorage internal/server/handler.go: each stream reads up to streaming_batch_size events
    after its last one from the event table; if it got some, it polls again 10 ms later and resets its backoff;
    if not, it waits an exponential backoff that starts at streaming_interval, grows 1.5x per empty poll, is
    randomized +-50% and is capped at a minute. The table query itself is taken as free, which flatters the model.
    """

    def __init__(self, interval_s: float, seed: str):
        self.initial, self.cur = interval_s, interval_s
        self.rng = random.Random(seed)
        self.next_ns = None
        self.waiting = []                 # (ns, event) recorded by ChainStorage, not yet seen by this model

    def add(self, ns: int, e: dict):
        self.waiting.append((ns, e))
        if self.next_ns is None:
            self.next_ns = ns + int(self.rng.random() * self.initial * 1e9)   # a stream already mid-loop

    def advance(self, upto_ns: int):
        """Run the loop's ticks up to upto_ns; return [(event, delivered_ns)]."""
        out = []
        while self.next_ns is not None and self.next_ns <= upto_ns:
            t = self.next_ns
            ready = [x for x in self.waiting if x[0] <= t]
            if ready:
                got = ready[:50]                # streaming_batch_size
                self.waiting = ready[50:] + [x for x in self.waiting if x[0] > t]
                out += [(e, t) for _, e in got]
                self.cur = self.initial
                self.next_ns = t + 10_000_000
            else:
                d = self.cur * 0.5
                wait = self.cur - d + self.rng.random() * 2 * d
                self.cur = min(self.cur * 1.5, 60.0)
                self.next_ns = t + int(wait * 1e9)
        return out


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
