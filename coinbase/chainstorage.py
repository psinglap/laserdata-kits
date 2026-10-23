"""ChainStorage's event stream, written once to the chain event lane: every block added to each chain, and every
block removed from it by a reorg, as BLOCK_ADDED and BLOCK_REMOVED events with a per-chain sequence number.

This stands in for ChainStorage's poller and streamer workflows. Each chain produces blocks at its configured
block time (copied from the open-source ChainStorage configs), with a little jitter. Now and then a reorg
replaces the tip: the old blocks are removed, newest first, and the replacements added, so an indexer that
applies the events in order always holds the canonical chain (+1, +2, +3, -3, -2, +2', +3' gives +1, +2', +3').

On start it writes --history-s seconds of past events (the log an indexer can rebuild from), then goes live.
Writers are on their own connections, one per partition, and write every --tick-ms; an event recorded at some
point in the last tick waits for that write, and the wait counts in the latency the indexers measure.
--reorgs multiplies the illustrative reorg rates, so a short run sees enough removals to measure.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import (CHAINS, DEFAULT_URL, EVENTS, PARTITIONS, START_HEIGHT, STREAM, Window, block_hash, connect,
                    ensure_topics, msg, part_of)


class Chain:
    def __init__(self, name, rng):
        self.name, self.rng = name, rng
        self.block_time, _, _, self.reorg_p, self.max_depth = CHAINS[name]
        self.height = START_HEIGHT[name]
        self.fork = defaultdict(int)       # height -> how many times it has been replaced
        self.seq = 0
        self.due = 0.0

    def gap(self):
        if self.block_time >= 60:          # proof of work: exponential gaps between blocks
            return self.rng.expovariate(1 / self.block_time)
        return self.block_time * self.rng.uniform(0.95, 1.05)

    def ev(self, kind, height, ns, hist):
        self.seq += 1
        d = dict(chain=self.name, seq=self.seq, type=kind, height=height,
                 hash=block_hash(self.name, height, self.fork[height]), tag=1, skipped=False,
                 ts=round(ns / 1e9, 3), ns=ns)
        if hist:
            d["hist"] = 1
        return d

    def next_block(self, ns, reorg_mult, hist=False):
        """The events ChainStorage records when this chain's next block arrives."""
        out = []
        if self.rng.random() < self.reorg_p * reorg_mult:
            depth = self.rng.randint(1, self.max_depth)
            for h in range(self.height, self.height - depth, -1):
                out.append(self.ev("BLOCK_REMOVED", h, ns, hist))
            for h in range(self.height - depth + 1, self.height + 1):
                self.fork[h] += 1
                out.append(self.ev("BLOCK_ADDED", h, ns, hist))
        self.height += 1
        out.append(self.ev("BLOCK_ADDED", self.height, ns, hist))
        if len(self.fork) > 4096:
            self.fork = defaultdict(int, {h: f for h, f in self.fork.items() if h > self.height - 2048})
        return out


async def main(a):
    admin = await connect(a.url)
    await ensure_topics(admin)
    writers = [await connect(a.url) for _ in range(PARTITIONS)]     # partition p is written by writers[p]
    rng = random.Random(7)
    chains = [Chain(c, rng) for c in CHAINS]
    acks, st = Window(), defaultdict(int)

    async def send(events):
        out = defaultdict(list)              # one batch per partition: a chain's events stay in order
        for e in events:
            out[part_of(e["chain"])].append(SendMessage(msg(**e)))
        s0 = time.perf_counter()
        await asyncio.gather(*(writers[p].send_messages(STREAM, EVENTS, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))
        acks.add((time.perf_counter() - s0) * 1e3)

    # The log so far: --history-s seconds of past events, so a new indexer has something to rebuild from.
    now = time.time_ns()
    hist = []
    for c in chains:
        n = int(a.history_s / c.block_time)
        c.height -= n
        t = now - int(a.history_s * 1e9)
        for _ in range(n):
            t += int(c.gap() * 1e9)
            hist += c.next_block(min(t, now - 1), a.reorgs, hist=True)
    hist.sort(key=lambda e: e["ns"])
    for i in range(0, len(hist), 2000):
        await send(hist[i:i + 2000])
    print(f"[chainstorage] wrote {len(hist):,} past events ({a.history_s:g} s of history) for {len(chains)} chains",
          flush=True)

    t0 = time.monotonic()
    for c in chains:
        c.due = t0 + c.gap() * rng.random()  # chains are out of phase, as in life

    async def run():
        gc.freeze()                          # keep start-up tables out of the collector's way
        due = time.monotonic()
        while True:
            now_m = time.monotonic()
            events = []
            for c in chains:
                while c.due <= now_m:
                    waited = int((now_m - c.due) * 1e9)
                    ev = c.next_block(time.time_ns() - waited, a.reorgs)
                    st["reorgs"] += ev[0]["type"] == "BLOCK_REMOVED"
                    events += ev
                    c.due += c.gap()         # from the due time, not from now
            st["late_ms"] = max(st["late_ms"], 1000 * (now_m - due))
            if events:
                await send(events)
                st["events"] += len(events)
                st["added"] += sum(e["type"] == "BLOCK_ADDED" for e in events)
                st["removed"] += sum(e["type"] == "BLOCK_REMOVED" for e in events)
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def show():
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[chainstorage] {st['events']:>4} events/s  BLOCK_ADDED {st['added']}  BLOCK_REMOVED {st['removed']}  "
                  f"reorgs {st['reorgs']}  write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms  "
                  f"tick late max {st['late_ms']:.0f}ms", flush=True)
            st.clear()

    print(f"[chainstorage] {len(chains)} chains live, about {sum(1 / v[0] for v in CHAINS.values()):.1f} blocks/s, "
          f"reorgs at {a.reorgs:g}x the illustrative rate, {PARTITIONS} partitions keyed by chain", flush=True)
    await asyncio.gather(run(), show())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--history-s", type=float, default=3600, help="seconds of past events written at start")
    p.add_argument("--reorgs", type=float, default=10, help="multiplier on the illustrative reorg rates")
    p.add_argument("--tick-ms", type=float, default=2, help="the writers write once per this many ms")
    asyncio.run(main(p.parse_args()))
