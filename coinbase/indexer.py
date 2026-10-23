"""Indexers on the chain event lane: each one reads every chain's BLOCK_ADDED and BLOCK_REMOVED events in sequence
order and keeps the canonical chain near the tip, the way ChainStorage's stream example does.

Each indexer has its own connection and reads every partition directly, with its own offsets (no consumer group,
nothing committed per message), so adding an indexer never slows the others. Its checkpoint is the next offset
per partition, which is the chain's next sequence number; in production it is persisted, as ChainStorage's
examples persist theirs. An indexer that restarts (--restart-at) resumes from its checkpoint: the events recorded
while it was down wait in the log and are counted as held, not as live latency.

Each indexer checks every event: the sequence number must be the next one for its chain (else a gap), a
removal must remove the current tip and an addition must extend it (else a broken chain). Both must stay 0.

The business number: between a reorg and the moment an indexer applies the removal, tip reads are answered
from a block that is no longer on the chain. At --reads tip reads a second per chain, the kit prints how many
such reads each indexer served, and how many it would have served behind the published StreamChainEvents loop
(a 1 s interval with backoff when idle, modelled from ChainStorage's open-source handler; see common.PollModel).
It also prints how late new blocks reach an indexer on the lane and behind that loop.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import CHAINS, DEFAULT_URL, EVENTS, PARTITIONS, STREAM, Canon, PollModel, Window, connect, ensure_topics, load


class Indexer:
    def __init__(self, i):
        self.i = i
        self.canon = {c: Canon() for c in CHAINS}
        self.down_from = None              # the restart window: events recorded in it wait in the log
        self.down_until = None


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url))
    start_ns = time.time_ns()
    ixs = [Indexer(i) for i in range(a.indexers)]
    models = {c: PollModel(CHAINS[c][1], f"{a.name}:{c}") for c in CHAINS}
    fed = defaultdict(int)                 # last sequence fed to each chain's model
    lat, warm, head, st, run = Window(), Window(), Window(), defaultdict(float), defaultdict(int)
    poll_head = Window()
    caught = [None]
    restarted = [False]

    def apply(ix, ms, now):
        for m in ms:
            e = load(m.payload())
            cn = ix.canon[e["chain"]]
            if not cn.apply(e):
                continue
            ns = e["ns"]
            if caught[0] is None or e.get("hist") or ns < start_ns:
                warm.add((now - ns) / 1e6)
                run["replayed"] += 1       # rebuilding from the log, not live
                continue
            if ix.down_from is not None and ix.down_from <= ns < ix.down_until:
                st["held"] += 1            # recorded while this indexer was down: waited in the log
                continue
            d = (now - ns) / 1e6
            lat.add(d)
            st["events"] += 1
            if e["type"] == "BLOCK_ADDED":
                head.add(d)
            else:
                st["removed"] += 1
                st["lane_stale_s"] += d / 1000
            if e["seq"] > fed[e["chain"]]:  # the same event, once, into the model of the published loop
                fed[e["chain"]] = e["seq"]
                models[e["chain"]].add(ns, e)

    async def read(ix):
        who = Consumer.Single(f"{me}-{ix.i}")
        c = await connect(a.url)
        nxt = [None] * PARTITIONS
        restart_at = None
        while True:
            if a.restart_at and ix.i == 0 and not restarted[0] and caught[0] is not None:
                if restart_at is None:
                    restart_at = caught[0] + a.restart_at
                if time.monotonic() >= restart_at:
                    # The indexer goes down; on restart it opens a new connection and resumes from its checkpoint.
                    restarted[0] = True
                    ix.down_from = time.time_ns() - 500_000_000   # the last polls before going down, too
                    ix.down_until = time.time_ns() + int(a.restart_for * 1e9) + 10_000_000_000
                    print(f"[{a.name}] indexer 0 down for {a.restart_for:g} s, checkpoint "
                          f"{sum(cn.seq for cn in ix.canon.values()):,} events", flush=True)
                    await asyncio.sleep(a.restart_for)
                    c = await connect(a.url)
                    ix.down_until = time.time_ns()  # everything recorded before now was held in the log
                    print(f"[{a.name}] indexer 0 resumed from its checkpoint on a new connection", flush=True)
            full = False
            for p in range(PARTITIONS):
                ms = await c.poll_messages(STREAM, EVENTS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if nxt[p] is not None
                                           else PollingStrategy.First())
                if ms:
                    apply(ix, ms, time.time_ns())
                    nxt[p] = ms[-1].offset() + 1
                    full |= len(ms) == a.batch
            if not full:                   # a rebuild reads on at once; live, wait a moment between polls
                await asyncio.sleep(a.poll_ms / 1000)

    async def model():
        # Advance the model of the published loop to 0.3 s ago, by which time the lane has delivered every event
        # recorded up to then, so both paths are judged on the same events.
        while True:
            await asyncio.sleep(0.02)
            upto = time.time_ns() - 300_000_000
            for c, m in models.items():
                for e, t in m.advance(upto):
                    d = (t - e["ns"]) / 1e6
                    if e["type"] == "BLOCK_ADDED":
                        poll_head.add(d)
                    else:
                        st["poll_removed"] += 1
                        st["poll_stale_s"] += d / 1000

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                w = warm.flush()
                lat.flush()
                if w["n"] and w["max"] < a.budget_ms and run["seconds"] >= 1:
                    caught[0] = time.monotonic()
                    gc.freeze()            # keep the rebuilt chains out of the collector's way
                    tips = sum(len(cn.blocks) > 0 for cn in ixs[0].canon.values())
                    print(f"[{a.name}] rebuilt every chain from the log in {(time.time_ns() - start_ns) / 1e9:.2f}s: "
                          f"{run['replayed']:,} events read by {len(ixs)} indexers, {tips} chains at their tip", flush=True)
                else:
                    print(f"[{a.name}] catching up: {run['replayed']:,} events read from the log", flush=True)
                run["seconds"] += 1
                st.clear()
                continue
            r, h, ph = lat.flush(), head.flush(), poll_head.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            gaps = sum(cn.gaps for ix in ixs for cn in ix.canon.values())
            breaks = sum(cn.breaks for ix in ixs for cn in ix.canon.values())
            dups = sum(cn.dups for ix in ixs for cn in ix.canon.values())
            lane_reads = st["lane_stale_s"] / len(ixs) * a.reads
            poll_reads = st["poll_stale_s"] * a.reads
            print(f"[{a.name}] {int(st['events']):>4} events/s to {len(ixs)} indexers  chain-to-indexer p50 {r['p50']:5.2f}ms  "
                  f"p99 {r['p99']:5.2f}ms  p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"new blocks: lane p99 {h['p99']:.0f}ms, published loop p50 {ph['p50']:.0f}ms p99 {ph['p99']:.0f}ms  "
                  f"removals {int(st['poll_removed'])}  tip reads from a removed block: lane {lane_reads:.1f}, "
                  f"published loop {poll_reads:.0f}  held in the log {int(st['held'])}  "
                  f"gaps {gaps}  breaks {breaks}  dups {dups}", flush=True)
            st.clear()

    print(f"[{a.name}] {len(ixs)} indexers, each reading {len(CHAINS)} chains on {PARTITIONS} partitions; "
          f"{a.reads:g} tip reads/s per chain", flush=True)
    await asyncio.gather(report(), model(), *(read(ix) for ix in ixs))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="idx-1")
    p.add_argument("--indexers", type=int, default=8, help="indexers in this process, each on its own connection")
    p.add_argument("--budget-ms", type=float, default=50, help="p99 budget, ChainStorage to indexer")
    p.add_argument("--reads", type=float, default=200, help="tip reads a second per chain per indexer")
    p.add_argument("--restart-at", type=float, default=0, help="seconds after catching up that indexer 0 restarts; 0 = never")
    p.add_argument("--restart-for", type=float, default=3, help="seconds indexer 0 stays down")
    p.add_argument("--poll-ms", type=float, default=2, help="wait between polls, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=2000)
    asyncio.run(main(p.parse_args()))
