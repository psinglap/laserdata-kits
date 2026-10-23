"""Shared settings for the CoinDCX mark price lane kit: every futures mark price and every position change reaches
every liquidation worker in milliseconds, on Apache Iggy, so each second's mark batch is checked against every
position before the next one lands."""
import asyncio
import json
import time
import zlib

from apache_iggy import Consumer, IggyClient, PollingStrategy

STREAM = "futures-risk"
MARKS = "mark-prices"            # the lane: one record per instrument per mark batch, keyed by instrument
POSITIONS = "positions"          # position changes keyed by account, resulting state (fills, adds, closes)
LIQ = "liquidation-orders"       # what the liquidation workers send to the order path
EXPORT = "trade-export"          # a heavy, latency-tolerant stream on the same broker (fills to the lakehouse)
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
# Marks: 4 partitions. The whole batch goes out at once, one send per partition, and the sends on one connection
# queue behind each other, so fewer, larger sends get the last mark of the batch out sooner.
PARTITIONS = {MARKS: 4, POSITIONS: 16, LIQ: 8, EXPORT: 16}

# Pairs as CoinDCX Futures names them, with mark prices read from the public futures price endpoint on
# 22 Oct 2026 as starting points. The rest of the ~500 active instruments are generated alts.
MAJORS = {
    "B-BTC_USDT": 81342.87, "B-ETH_USDT": 2384.79, "B-SOL_USDT": 103.40, "B-XRP_USDT": 1.3026,
    "B-DOGE_USDT": 0.07859, "B-BNB_USDT": 731.17, "B-ADA_USDT": 0.2117, "B-LINK_USDT": 11.924,
    "B-AVAX_USDT": 9.103, "B-SUI_USDT": 0.9722, "B-LTC_USDT": 58.558, "B-TRX_USDT": 0.32782,
    "B-1000PEPE_USDT": 0.004393, "B-1000SHIB_USDT": 0.004938, "B-NEAR_USDT": 4.8807, "B-DOT_USDT": 1.1089,
    "B-TON_USDT": 1.6972, "B-ENA_USDT": 0.19932, "B-WIF_USDT": 0.1967, "B-AAVE_USDT": 146.36,
}

# Maintenance margin tiers from the CoinDCX Futures API docs (dynamic_safety_margin_details): position size in
# USDT up to the key, at that percentage. A 60K USDT position needs 50K x 1.5% + 10K x 2% = 950 USDT.
MM_TIERS = [(50_000, 1.5), (100_000, 2.0), (500_000, 3.0), (1_000_000, 5.0), (5_000_000, 6.0), (15_000_000, 10.0)]


def instruments(n: int) -> list:
    names = list(MAJORS)[:n]
    return names + [f"B-ALT{i:03d}_USDT" for i in range(n - len(names))]


def start_price(pair: str) -> float:
    return MAJORS.get(pair) or 0.01 * (1 + zlib.crc32(pair.encode()) % 5000)


def account(i: int) -> str:
    return f"U{i:08d}"


def part_of(k: str, partitions: int) -> int:
    # The one partition function used everywhere: an instrument's marks, or an account's positions, always land in
    # one partition, in order.
    return zlib.crc32(k.encode()) % partitions


def maintenance_margin(notional: float) -> float:
    mm, lo = 0.0, 0.0
    for hi, pct in MM_TIERS:
        mm += max(0.0, min(notional, hi) - lo) * pct / 100
        lo = hi
        if notional <= hi:
            break
    return mm


def liq_price(side: str, entry: float, qty: float, margin: float) -> float:
    # Isolated margin: the price at which the margin left in the position falls to the maintenance margin.
    room = (margin - maintenance_margin(entry * qty)) / qty
    return entry - room if side == "long" else entry + room


def bankruptcy_price(side: str, entry: float, qty: float, margin: float) -> float:
    # The price at which the position's margin is gone; any worse fill is paid by the insurance fund.
    return entry - margin / qty if side == "long" else entry + margin / qty


def upnl(side: str, entry: float, qty: float, mark: float) -> float:
    return (mark - entry) * qty if side == "long" else (entry - mark) * qty


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


async def ensure_topics(c: IggyClient, *topics):
    """Create the stream and topics if they are missing, so any script can start first."""
    if await c.get_stream(STREAM) is None:
        try:
            await c.create_stream(STREAM)
        except RuntimeError:
            pass                          # created by another process in the meantime
    for t in topics:
        if await c.get_topic(STREAM, t) is None:
            try:
                await c.create_topic(STREAM, t, PARTITIONS[t])
            except RuntimeError:
                pass
    while not all([await c.get_topic(STREAM, t) for t in topics]):
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
