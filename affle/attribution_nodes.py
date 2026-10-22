"""Click trackers and attribution nodes: every click and install postback onto the lane, every block applied
before the blocked publisher's next conversion is billed.

Plays --nodes attribution nodes in one process. Clicks arrive at --clicks a second from --publishers genuine
publishers over --devices devices; --install-rate of them lead to an install --ctit-min to --ctit-max seconds
later (illustrative, compressed). Each device belongs to one node, which publishes its clicks and install
postbacks to click-install, keyed by device, with that node's sequence for the device, and attributes each
install to the device's last click (last-click attribution).

Click injection is planted here (the kit's ground truth): every --incident-every seconds a new publisher starts
firing a click on --hijack of the devices whose install is beginning, --inject-min to --inject-max seconds before
the install postback, so the install is attributed to it. Each conversion it takes is a CPCU conversion billed
to the advertiser for an install the publisher did not drive.

Each node reads publisher-blocks directly on its own connection. A block moves that publisher's next
conversions from billed to held on that node.

The business number: hijacked conversions billed, and of those, how many were billed after the scorer had
already blocked the publisher, because the block had not reached that node yet, in conversions and rupees at the
average CPCU. Prints it every second, with what a --compare-ms lane would have billed, and block-to-nodes
p50/p99: the second hop of the loop.
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

from common import (BLOCKED, BLOCKS, CPCU_INR, DEFAULT_URL, EVENTS, PARTITIONS, STREAM, Window, device,
                    ensure_topics, honest_ctit, load, msg, part, publisher)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), EVENTS, BLOCKS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, EVENTS, topic_partitions_count=PARTITIONS[EVENTS],
                                                   mode=cfg)
    start_ns = time.time_ns()
    nodes = [f"node-{i:02d}" for i in range(a.nodes)]
    honest = [publisher(i) for i in range(a.publishers)]
    fraud = set()                           # publishers that inject clicks (ground truth)
    node_of = lambda d: nodes[int(d[4:]) % a.nodes]
    last = {}                               # device -> publisher of its last click
    due = []                                # (time, device, "start" or "post") installs to come
    seq = defaultdict(int)                  # (node, device) -> sequence
    blocked = {w: {} for w in nodes}        # node -> {publisher: block ns}, as that node knows it
    billed = {w: defaultdict(deque) for w in nodes}   # node -> {publisher: ns of recent billed conversions}
    cmp_until = {w: {} for w in nodes}      # node -> {publisher: block ns + compare window}
    incident = {"pub": None, "onset": 0, "end": 0.0, "got": {}, "block_ns": 0, "billed": 0}
    lat, stats = Window(), defaultdict(float)

    def on_block(w, d):
        pub = d["pub"]
        if pub in blocked[w] or d["state"] != BLOCKED:
            return                          # repeated block: already applied
        blocked[w][pub] = d["ns"]
        if d["ns"] <= start_ns:
            return                          # a block from before this run, replayed at start-up
        lat.add((time.time_ns() - d["ns"]) / 1e6)
        if pub not in fraud:
            stats["false"] += 1
            print(f"[nodes] FALSE BLOCK {pub} on {w}", flush=True)
        # Conversions this node billed to the publisher after the block existed, before it arrived here.
        blind = sum(1 for t in billed[w][pub] if t >= d["ns"])
        stats["blind"] += blind
        stats["at_compare"] += blind
        cmp_until[w][pub] = d["ns"] + int(a.compare_ms * 1e6)
        if incident["pub"] == pub:
            incident["got"][w] = time.time_ns()
            incident["block_ns"] = incident["block_ns"] or d["ns"]
            if len(incident["got"]) == len(nodes):
                lastn = max(incident["got"].values())
                print(f"[nodes] incident {pub}: onset to block {(incident['block_ns'] - incident['onset']) / 1e6:.0f} ms, "
                      f"block to all {len(nodes)} nodes {(lastn - incident['block_ns']) / 1e6:.1f} ms, "
                      f"hijacked conversions billed before the block {incident['billed']:,} "
                      f"(Rs {incident['billed'] * CPCU_INR:,.0f})", flush=True)
                stats["incidents"] += 1

    async def read_blocks(w):
        # The one block partition, from the start of the log (every node rebuilds its block list), then from its
        # own position; nothing is stored on the broker.
        c = await own_client()
        who = Consumer.Single(f"{w}-{os.getpid()}")
        nxt = None
        while True:
            ms = await c.poll_messages(STREAM, BLOCKS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    on_block(w, load(m.payload()))
            else:
                await asyncio.sleep(a.poll_ms / 1000)

    gc.freeze()                             # keep start-up objects out of the collector's way

    def emit(batches, now_ns, w, dev, **f):
        seq[(w, dev)] += 1
        batches[part(dev, PARTITIONS[EVENTS])].append(SendMessage(msg(d=dev, w=w, seq=seq[(w, dev)], ns=now_ns, **f)))

    async def tracker_loop():
        t0 = tick = last_t = time.monotonic()
        owed, n_inc = 0.0, 0
        async with producer:
            while True:
                now = time.monotonic()
                el = now - t0
                x = a.peak if a.peak_at <= el < a.peak_at + a.peak_for else 1
                if incident["pub"] and now >= incident["end"]:
                    incident["pub"] = None
                start_at = a.incident_at + n_inc * a.incident_every
                if a.incident_every and el >= start_at and not incident["pub"]:
                    pub = publisher(a.publishers + n_inc)
                    fraud.add(pub)
                    incident.update(pub=pub, onset=time.time_ns(), end=now + a.incident_for, got={}, block_ns=0,
                                    billed=0)
                    n_inc += 1
                now_ns = time.time_ns()
                batches = defaultdict(list)
                owed += a.clicks * x * (now - last_t)   # what this tick owes, so a slow tick is made up next
                last_t = now
                n, owed = int(owed), owed - int(owed)
                for _ in range(n):
                    dev = device(random.randrange(a.devices))
                    pub = random.choice(honest)
                    last[dev] = pub
                    emit(batches, now_ns, node_of(dev), dev, e="c", pub=pub, rd=random.randint(40, 400))
                    if random.random() < a.install_rate:
                        heapq.heappush(due, (now + honest_ctit(a.ctit_min, a.ctit_max), dev, "start"))
                    stats["clicks"] += 1
                while due and due[0][0] <= now:
                    _, dev, kind = heapq.heappop(due)
                    w = node_of(dev)
                    if kind == "start" and incident["pub"] and random.random() < a.hijack:
                        # The install begins; an injected click lands just before its postback.
                        last[dev] = incident["pub"]
                        emit(batches, now_ns, w, dev, e="c", pub=incident["pub"], rd=random.randint(5, 30))
                        heapq.heappush(due, (now + random.uniform(a.inject_min, a.inject_max), dev, "post"))
                        stats["clicks"] += 1
                        continue
                    pub = last[dev]
                    emit(batches, now_ns, w, dev, e="i", app=f"app-{int(dev[4:]) % 40:02d}")
                    stats["installs"] += 1
                    if cmp_until[w].get(pub, 0) > now_ns:
                        stats["at_compare"] += 1    # a slower lane would still be billing this conversion
                    if pub in blocked[w]:
                        stats["held"] += 1
                        continue
                    stats["billed"] += 1
                    dq = billed[w][pub]
                    dq.append(now_ns)
                    while dq and dq[0] < now_ns - 10_000_000_000:
                        dq.popleft()
                    if pub in fraud:
                        stats["hijacked"] += 1
                        if pub == incident["pub"]:
                            incident["billed"] += 1
                # One batch per partition, all sent at once: a device's click stays ahead of its install.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["peak"] = x > 1
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[nodes] {a.nodes} attribution nodes, ~{a.clicks:,.0f} clicks/s from {a.publishers:,} publishers, "
              f"{a.install_rate:.1%} install, click injection every {a.incident_every:g} s for {a.incident_for:g} s",
              flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            print(f"[nodes] {int(stats['clicks']):>7,} clicks/s ({phase})  installs {int(stats['installs']):,}  "
                  f"billed {int(stats['billed']):,}  held {int(stats['held']):,}  hijacked billed "
                  f"{int(stats['hijacked']):,}, after its block was out {int(stats['blind']):,} "
                  f"(at {a.compare_ms:g} ms it would be {int(stats['at_compare']):,}, "
                  f"Rs {stats['at_compare'] * CPCU_INR:,.0f})  false blocks {int(stats['false'])}  "
                  f"block-to-nodes n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("clicks", "installs", "billed", "held", "hijacked", "blind", "at_compare"):
                stats[k] = 0

    await asyncio.gather(tracker_loop(), report(), *(read_blocks(w) for w in nodes))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--nodes", type=int, default=8, help="attribution nodes, each with its own block reader")
    p.add_argument("--clicks", type=float, default=10_000, help="clicks per second across all nodes")
    p.add_argument("--devices", type=int, default=1_000_000, help="devices the clicks come from")
    p.add_argument("--publishers", type=int, default=500, help="genuine publishers")
    p.add_argument("--install-rate", type=float, default=0.01, help="share of clicks followed by an install")
    p.add_argument("--ctit-min", type=float, default=11, help="shortest genuine click-to-install, seconds")
    p.add_argument("--ctit-max", type=float, default=30, help="longest genuine click-to-install, seconds")
    p.add_argument("--incident-at", type=float, default=30, help="seconds after start of the first injection")
    p.add_argument("--incident-every", type=float, default=10, help="seconds between injecting publishers (0 = none)")
    p.add_argument("--incident-for", type=float, default=8, help="seconds each publisher injects")
    p.add_argument("--hijack", type=float, default=0.6, help="share of beginning installs that get an injected click")
    p.add_argument("--inject-min", type=float, default=0.5, help="injected click to install postback, seconds")
    p.add_argument("--inject-max", type=float, default=4.0)
    p.add_argument("--poll-ms", type=float, default=1, help="block readers' wait when nothing is new")
    p.add_argument("--compare-ms", type=float, default=1000, help="also count conversions a lane this slow would bill")
    p.add_argument("--peak", type=float, default=1, help="click-rate multiplier during a peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
