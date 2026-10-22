"""The Kill Bill writer: drain the usage lane into Kill Bill, and keep usage in the log while Kill Bill is down.

Plays the role of the Kill Bill Kafka consumer plugin on the lane. It reads the usage topic directly, partition by
partition, on its own connection, at its own pace, and records each message's unitUsageRecords against the
subscription in Kill Bill (here a stand-in, KillBill below, that records usage by subscription, unit type and
recordDate and refuses a trackingId it has already recorded, the deduplication key the plugin's README names).

--outage-at / --outage-for take Kill Bill down for a while. The writer keeps the failed batch, retries once a
second and stops reading, so usage waits in Apache Iggy, not in memory and not in the payment services. When
Kill Bill is back it drains the backlog from its own position, in order per subscription. The live meter
(meter.py) reads the same topic on its own connections and is not held up by any of this.

Prints, every second: usage recorded, usage-to-Kill-Bill p50/p99, the oldest unrecorded usage, duplicates
refused, and the reconciliation: for every subscription, what Kill Bill recorded for the day against the day
totals the payment services sent, to the paisa.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, STREAM, USAGE, Window, amounts, ensure_topics, load, rupees


class KillBill:
    """Stand-in for Kill Bill's usage API: records usage per (tenant, subscription, unitType, recordDate)."""

    def __init__(self, a):
        self.a, self.t0 = a, time.monotonic()
        self.seen = set()                   # trackingIds already recorded
        self.usage = defaultdict(int)       # (subscription, recordDate, unitType) -> amount
        self.refused = 0

    def down(self):
        t = time.monotonic() - self.t0
        return self.a.outage_for > 0 and self.a.outage_at <= t < self.a.outage_at + self.a.outage_for

    async def record(self, batch):
        await asyncio.sleep(self.a.call_ms / 1000 + len(batch) / self.a.kb_rps)
        if self.down():
            raise ConnectionError("Kill Bill unavailable")
        for u in batch:
            if u["trackingId"] in self.seen:
                self.refused += 1           # already recorded: a retry or a replay
                continue
            self.seen.add(u["trackingId"])
            for unit in u["unitUsageRecords"]:
                for r in unit["usageRecords"]:
                    self.usage[(u["subscriptionExternalKey"], r["recordDate"], unit["unitType"])] += r["amount"]


async def main(a):
    c = IggyClient.from_connection_string(a.url)        # its own connection: never shares one with the meter
    await c.connect()
    await ensure_topics(c, USAGE)
    kb = KillBill(a)
    who = Consumer.Single(f"killbill-writer-{os.getpid()}")
    parts = list(range((await c.get_topic(STREAM, USAGE)).partitions_count))
    lat, stats = Window(), defaultdict(float)
    sent = {}                               # subscription -> (seq, day, cnt, vol): highest day totals recorded
    pending = []                            # read from the log, not yet confirmed by Kill Bill
    st = {"mode": "catching up", "down_since": None, "backlog_s": 0.0, "drain_t0": None, "drained": 0,
          "outages": 0}
    gc.freeze()

    async def read():
        nxt = {}
        while True:
            if len(pending) >= a.max_pending:
                await asyncio.sleep(0.005)  # Kill Bill is behind or down: stop reading, usage waits in the log
                continue
            more = False
            for p in parts:
                ms = await c.poll_messages(STREAM, USAGE, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    pending.extend(load(m.payload()) for m in ms)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def write():
        while True:
            await asyncio.sleep(a.flush_ms / 1000)
            if not pending:
                continue
            batch = pending[:a.max_call]
            try:
                await kb.record(batch)
            except ConnectionError:
                if st["down_since"] is None:
                    st["down_since"] = time.monotonic()
                    st["outages"] += 1
                await asyncio.sleep(1)      # keep the batch, retry; nothing is committed or dropped
                continue
            done = time.time_ns()
            if st["down_since"] is not None:            # Kill Bill is back: drain what waited in the log
                st["down_since"] = None
                st["mode"], st["drain_t0"], st["drained"] = "draining", time.monotonic(), 0
                st["backlog_s"] = (done - batch[0]["ns"]) / 1e9    # how far behind Kill Bill came back
            del pending[:len(batch)]
            for u in batch:
                s = u["subscriptionExternalKey"]
                if s not in sent or u["seq"] > sent[s][0]:
                    sent[s] = (u["seq"], amounts(u)[0], u["cnt"], u["vol"])
                if st["mode"] == "live":
                    lat.add((done - u["ns"]) / 1e6)
                elif st["mode"] == "draining":
                    st["drained"] += 1
                else:
                    stats["replayed"] += 1
            stats["recorded"] += len(batch)
            stats["oldest"] = max(stats["oldest"], (done - batch[0]["ns"]) / 1e9)

    def reconcile():
        ok = bad = 0
        for s, (_, day, cnt, vol) in sent.items():
            if (kb.usage[(s, day, "txnCount")], kb.usage[(s, day, "txnVolume")]) == (cnt, vol):
                ok += 1
            else:
                bad += 1
        return ok, bad

    async def report():
        while True:
            await asyncio.sleep(1)
            ok, bad = reconcile()
            tail = (f"duplicates refused {kb.refused:,}  subscriptions reconciled {ok:,}/{ok + bad:,}  "
                    f"txnVolume recorded {rupees(sum(v for k, v in kb.usage.items() if k[2] == 'txnVolume'))}")
            if st["down_since"] is not None:
                print(f"[killbill] Kill Bill down for {time.monotonic() - st['down_since']:4.1f}s: reading paused, "
                      f"usage waits in Apache Iggy  {tail}", flush=True)
            elif st["mode"] == "live":
                r = lat.flush()
                print(f"[killbill] live: recorded {int(stats['recorded']):>7,} usage msgs/s  usage-to-Kill-Bill "
                      f"p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  {tail}", flush=True)
            else:
                print(f"[killbill] {st['mode']}: recorded {int(stats['recorded']):,} this second, oldest "
                      f"{stats['oldest']:5.1f}s old  {tail}", flush=True)
                if stats["recorded"] and stats["oldest"] < a.live_s:
                    if st["mode"] == "draining":
                        took = time.monotonic() - st["drain_t0"]
                        print(f"[killbill] outage over: {st['drained']:,} usage messages ({st['backlog_s']:.1f}s of payments) "
                              f"drained in {took:.1f}s, nothing lost, nothing recorded twice", flush=True)
                    st["mode"] = "live"
                    lat.flush()
            stats.clear()

    await asyncio.gather(read(), write(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--batch", type=int, default=2000, help="messages per poll, per partition")
    p.add_argument("--poll-ms", type=float, default=2)
    p.add_argument("--flush-ms", type=int, default=50, help="one Kill Bill call at most this often")
    p.add_argument("--max-call", type=int, default=5000, help="usage messages per Kill Bill call, at most")
    p.add_argument("--max-pending", type=int, default=20000, help="stop reading above this many unconfirmed messages")
    p.add_argument("--call-ms", type=float, default=15, help="Kill Bill call overhead in the stand-in")
    p.add_argument("--kb-rps", type=float, default=50000, help="usage records per second the stand-in accepts")
    p.add_argument("--live-s", type=float, default=1.0, help="back to live once the oldest recorded usage is younger")
    p.add_argument("--outage-at", type=float, default=30, help="Kill Bill goes down this many seconds after start")
    p.add_argument("--outage-for", type=float, default=0, help="seconds; 0 = no outage")
    asyncio.run(main(p.parse_args()))
