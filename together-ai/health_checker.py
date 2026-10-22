"""The GPU node health checker, fed by the node-signals lane.

Reads node-signals directly, partition by partition, on --readers connections of its own (no consumer group: a
group reader visits one partition per poll, so with 16 partitions a sample waits for its partition's turn). Each
record is one passive health sample from one node: the signals active on it now (empty when healthy), using the
names in Together's health check docs, and the node agent's sequence, so a replayed or repeated sample is
recognised and changes nothing.

For each node it decides (decide() is where your rule goes):
  * HOLD at once on a fault signal (an Xid, an uncorrectable ECC error, a GPU fallen off the bus, a kernel deadlock),
    and on a condition signal (thermal throttle, PCIe replays, IB link flapping) seen in --min-samples samples
    within --window-s seconds. The hold carries the repair action Together's docs map to that signal (reboot,
    quick reprovision, migrate), for the repair recommendation; the drain itself still waits for approval.
  * OK again after --clear-samples clean samples in a row.
Warning-only signals (disk pressure) are counted, never held. Each change is published on node-health, which every
placement scheduler reads (fleet.py). A decision carries the node and the resulting state (never "toggle"), and the
newest per node wins, so a restarted checker rebuilds every node's state from the log.

Prints sample-to-checker p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the decisions.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (DEFAULT_URL, HEALTH, HOLD, OK, PARTITIONS, SIGNALS, STREAM, SUSTAINED, WARN, Window, action_for,
                    ensure_topics, load, msg)


class Node:
    """One node's recent condition signals and its run of clean samples."""
    __slots__ = ("seen", "clean", "state")

    def __init__(self):
        self.seen = defaultdict(deque)      # condition signal -> ns of recent samples carrying it
        self.clean = 0
        self.state = OK


def decide(n: Node, sigs, ns, a):
    """Stands in for your health rule. Returns (new state, signal, action) or None when nothing changes."""
    if not sigs:
        n.clean += 1
        if n.state == HOLD and n.clean >= a.clear_samples:
            return OK, "", ""
        return None
    n.clean = 0
    if n.state == HOLD:
        return None
    for s in sigs:
        name, xid = s["s"], s.get("x", 0)
        act = action_for(name, xid)
        if act == WARN:
            continue
        if name in SUSTAINED:
            q = n.seen[name]
            q.append(ns)
            while q and q[0] < ns - a.window_s * 1e9:
                q.popleft()
            if len(q) < a.min_samples:
                continue
        return HOLD, name, act
    return None


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SIGNALS, HEALTH)
    out = await (await own_client()).producer(STREAM, HEALTH, topic_partitions_count=PARTITIONS[HEALTH])
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # node -> last sequence applied
    nodes = defaultdict(Node)
    pending = []                            # decisions waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays samples from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_sample(m):
        d = load(m.payload())
        age = (time.time_ns() - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest sample seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = d["k"]
        if d["seq"] <= seq.get(k, 0):
            stats["dropped"] += 1           # older or repeated sample: already applied
            return
        seq[k] = d["seq"]
        stats["samples"] += 1
        sigs = d["sig"]
        if sigs:
            stats["signals"] += 1
            stats["warnings"] += any(action_for(s["s"], s.get("x", 0)) == WARN for s in sigs)
        n = nodes[k]
        r = decide(n, sigs, d["ns"], a)
        if r is None:
            return
        state, sig, act = r
        n.state = state
        n.seen.clear()
        stats[state] += 1
        if age > a.replay_decision_s * 1000:
            return                          # replayed from long ago: state rebuilt, nothing to tell the schedulers
        pending.append(SendMessage(msg(k=k, state=state, sig=sig, action=act, sns=d["ns"])))
        if caught_up[0]:
            print(f"[checker] {k} -> {state.upper()}" + (f" {sig}, recommend {act}" if sig else ""), flush=True)

    async def send_decisions():
        # Decisions are small and rare; one batch every 2 ms keeps them off a send-per-decision loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"checker-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, SIGNALS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_sample(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of samples arrived inside the budget; partitions interleave,
                # so one fresh sample alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[checker] catching up: replayed {int(stats['replayed']):,} earlier samples this second, "
                      f"state rebuilt for {len(nodes):,} nodes", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            held = sum(1 for n in nodes.values() if n.state == HOLD)
            print(f"[checker] {r['n']:>7,} samples/s  sample-to-checker p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  with signals {int(stats['signals']):,}  "
                  f"warnings {int(stats['warnings']):,}  decisions sent {int(stats['sent']):,} "
                  f"(hold {int(stats[HOLD]):,}, ok {int(stats[OK]):,})  nodes on hold {held:,}", flush=True)
            stats.clear()

    parts = list(range((await (await own_client()).get_topic(STREAM, SIGNALS)).partitions_count))
    await asyncio.gather(send_decisions(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the node-signals partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--budget-ms", type=int, default=50, help="sample-to-checker p99 budget for the lane")
    p.add_argument("--window-s", type=float, default=5.0, help="seconds a condition signal is counted over")
    p.add_argument("--min-samples", type=int, default=3, help="samples with a condition signal before a hold")
    p.add_argument("--clear-samples", type=int, default=3, help="clean samples in a row before a node is ok again")
    p.add_argument("--replay-decision-s", type=float, default=10,
                   help="after a restart, still publish decisions for samples younger than this")
    asyncio.run(main(p.parse_args()))
