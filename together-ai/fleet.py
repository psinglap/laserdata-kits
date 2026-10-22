"""A GPU inference fleet: every node's passive health samples onto the lane, and every hold applied by every
placement scheduler before its next placement.

Node agents: --nodes GPU nodes, each sending one passive health sample every --sample-s seconds to node-signals,
keyed by node: the signals active on it now, by the names in Together's health check docs (empty when healthy),
and the agent's sequence for that node. A few nodes carry warning-only disk pressure in the background.

Faults are planted here (the kit's ground truth): every --fault-every seconds one node with free GPUs gets a fault
(an Xid, an uncorrectable ECC error, a GPU fallen off the bus, IB link flapping, thermal throttle, ...) that stays
in its samples for --fault-for seconds, standing in for the time until its repair is approved and done.

Placement schedulers: --schedulers bin-packers place --placements new replicas and jobs a second on the --open
nodes that have free GPUs. A faulted node's pods fail and its GPUs look free, so a bin-packer leans toward it
(--pull of placements while it is faulted; illustrative). Each scheduler reads node-health directly on its own
connection and re-picks while a node is on HOLD; OK lets placements land there again.

The business number: placements onto a faulted node, and of those, how many landed after the checker had already
put it on hold, because the hold had not reached that scheduler yet. Prints it every second, with what a
--compare-ms path (a polling loop or an alert evaluation interval) would have let through, judged on the same
picks, and hold-to-schedulers p50/p99: the second hop of the loop.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, HEALTH, HOLD, PARTITIONS, PLANTED, SIGNALS, STREAM, Window, ensure_topics, load, msg,
                    node_id, part)

XID = {"DmesgXidError": 48, "DmesgGpuFallenOffBus": 79}   # Xid codes carried with these signals (illustrative)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SIGNALS, HEALTH)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, SIGNALS, topic_partitions_count=PARTITIONS[SIGNALS],
                                                   mode=cfg)
    start_ns = time.time_ns()
    rnd = random.Random(11)
    nodes = [node_id(i) for i in range(a.nodes)]
    pid = [part(k, PARTITIONS[SIGNALS]) for k in nodes]
    open_nodes = rnd.sample(range(a.nodes), min(a.open, a.nodes))   # nodes with free GPUs right now
    noisy = set(rnd.sample(range(a.nodes), max(1, a.nodes // 500)))  # background disk-pressure warnings
    scheds = [f"sched-{i:02d}" for i in range(a.schedulers)]
    seq = [0] * a.nodes
    fault = {}                              # node index -> (signal, end monotonic)
    view = {w: {} for w in scheds}          # scheduler -> {node: hold or ok}, as that scheduler knows it
    vns = {w: {} for w in scheds}           # scheduler -> {node: ns of the decision it applied}
    placed = {w: defaultdict(deque) for w in scheds}   # scheduler -> {node: ns of placements onto a faulted node}
    cmp_until = {w: {} for w in scheds}     # scheduler -> {node: hold ns + compare window}
    incidents = {}                          # node -> {"onset", "hold_ns", "got", "hit", "sig"}
    lat, stats = Window(), defaultdict(float)

    def on_decision(w, d):
        k = int(d["k"].rsplit("-", 1)[1])          # gpu-node-00042 -> 42
        if d["ns"] <= vns[w].get(k, 0):
            return                          # older or repeated decision: the newest wins
        vns[w][k] = d["ns"]
        view[w][k] = d["state"]
        live = d["ns"] > start_ns
        if live:
            lat.add((time.time_ns() - d["ns"]) / 1e6)   # live decisions only, not ones replayed at start-up
        if d["state"] != HOLD or not live:
            return
        # Placements this scheduler made onto the node after the hold existed, before it arrived here.
        blind = sum(1 for t in placed[w][k] if t >= d["ns"])
        stats["blind"] += blind
        stats["at_compare"] += blind
        cmp_until[w][k] = d["ns"] + int(a.compare_ms * 1e6)
        inc = incidents.get(k)
        if inc is not None:
            inc["got"][w] = time.time_ns()
            inc["hold_ns"] = inc["hold_ns"] or d["ns"]
            if len(inc["got"]) == len(scheds):
                last = max(inc["got"].values())
                print(f"[fleet] incident {d['k']} {inc['sig']}: onset to hold {(inc['hold_ns'] - inc['onset']) / 1e6:.0f} ms "
                      f"(recommend {d.get('action')}), hold to all {len(scheds)} schedulers "
                      f"{(last - inc['hold_ns']) / 1e6:.1f} ms, placements that landed on it first {inc['hit']:,}", flush=True)
                stats["incidents"] += 1
                del incidents[k]

    async def read_health(w):
        # The single node-health partition, read directly from the start (state is rebuilt from it), then from
        # our own position; nothing is stored on the broker.
        c = await own_client()
        who = Consumer.Single(f"{w}-{os.getpid()}")
        nxt = None
        while True:
            ms = await c.poll_messages(STREAM, HEALTH, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    on_decision(w, load(m.payload()))
            if not ms or len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)

    gc.freeze()                             # keep start-up objects out of the collector's way

    async def agents():
        now = time.monotonic()
        due = [(now + rnd.uniform(0, a.sample_s), i) for i in range(a.nodes)]
        heapq.heapify(due)
        t0 = tick = time.monotonic()
        n_fault = 0
        async with producer:
            while True:
                now = time.monotonic()
                el = now - t0
                for i in [i for i, (_, end) in fault.items() if now >= end]:
                    del fault[i]
                if a.fault_every and el >= a.fault_at + n_fault * a.fault_every:
                    i = rnd.choice([j for j in open_nodes if j not in fault])
                    sig = PLANTED[n_fault % len(PLANTED)]
                    fault[i] = (sig, now + a.fault_for)
                    incidents[i] = {"onset": time.time_ns(), "hold_ns": 0, "got": {}, "hit": 0, "sig": sig}
                    n_fault += 1
                batches = defaultdict(list)
                while due and due[0][0] <= now:
                    _, i = heapq.heappop(due)
                    sig = []
                    if i in fault:
                        s = fault[i][0]
                        sig.append({"s": s, "g": i % 8, "x": XID[s]} if s in XID else {"s": s, "g": i % 8})
                    elif i in noisy and rnd.random() < 0.3:
                        sig.append({"s": "KubeNodeDiskPressure"})
                    seq[i] += 1
                    batches[pid[i]].append(SendMessage(msg(k=nodes[i], seq=seq[i], sig=sig)))
                    stats["samples"] += 1
                    heapq.heappush(due, (now + a.sample_s, i))
                # One batch per partition, all sent at once: per-node order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def placements():
        owed, last, tick = 0.0, time.monotonic(), time.monotonic()
        while True:
            now = time.monotonic()
            owed += a.placements * (now - last)
            last, n = now, int(owed)
            owed -= n
            now_ns = time.time_ns()
            faulted_open = [i for i in open_nodes if i in fault]
            for _ in range(n):
                w = scheds[rnd.randrange(len(scheds))]
                # The bin-packer's pick before any health view is applied, so the live lane and the slower
                # path are judged on the same choice.
                p = rnd.choice(faulted_open) if faulted_open and rnd.random() < a.pull else rnd.choice(open_nodes)
                if p in fault and cmp_until[w].get(p, 0) > now_ns:
                    stats["at_compare"] += 1    # a slower path would still be placing here
                f, tries = p, 0
                while view[w].get(f) == HOLD and tries < 20:
                    f, tries = rnd.choice(open_nodes), tries + 1
                stats["placed"] += 1
                if f in fault:
                    stats["hit"] += 1
                    dq = placed[w][f]
                    dq.append(now_ns)
                    while dq and dq[0] < now_ns - 30_000_000_000:
                        dq.popleft()
                    if f in incidents and view[w].get(f) != HOLD:
                        incidents[f]["hit"] += 1
            tick += 0.01
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[fleet] {a.nodes:,} GPU nodes sampling every {a.sample_s:g} s (~{a.nodes / a.sample_s:,.0f} samples/s), "
              f"{a.schedulers} schedulers placing {a.placements:g}/s on {len(open_nodes)} nodes with free GPUs, "
              f"a fault every {a.fault_every:g} s for {a.fault_for:g} s", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            print(f"[fleet] {int(stats['samples']):>7,} samples/s  placements {int(stats['placed']):,}  "
                  f"on a faulted node {int(stats['hit']):,}, after its hold was out {int(stats['blind']):,} "
                  f"(at {a.compare_ms:g} ms it would be {int(stats['at_compare']):,})  nodes faulted {len(fault)}  "
                  f"hold-to-schedulers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("samples", "placed", "hit", "blind", "at_compare"):
                stats[k] = 0

    await asyncio.gather(agents(), placements(), report(), *(read_health(w) for w in scheds))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--nodes", type=int, default=4000, help="GPU nodes, each with a node agent")
    p.add_argument("--sample-s", type=float, default=1.0, help="seconds between a node's passive health samples")
    p.add_argument("--schedulers", type=int, default=8, help="placement schedulers, each with its own health reader")
    p.add_argument("--placements", type=float, default=100, help="new replicas and jobs placed per second")
    p.add_argument("--open", type=int, default=64, help="nodes with free GPUs, where placements land")
    p.add_argument("--pull", type=float, default=0.3, help="share of placements a faulted node's free-looking GPUs draw")
    p.add_argument("--fault-at", type=float, default=15, help="seconds after start of the first planted fault")
    p.add_argument("--fault-every", type=float, default=10, help="seconds between planted faults (0 = none)")
    p.add_argument("--fault-for", type=float, default=20, help="seconds a fault stays until the node is repaired")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between health polls when nothing is waiting")
    p.add_argument("--compare-ms", type=float, default=15000,
                   help="also count placements a path this slow would let through (a polling or alert interval)")
    asyncio.run(main(p.parse_args()))
