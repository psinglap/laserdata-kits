"""A Propeller node, as the lane sees it: reads every member event from the lane, pushes each one down the gRPC
channel of every device of that member connected here, and replays missed events from the log when a device
reconnects.

Every node reads every partition directly, one connection per partition, with its own offsets (no consumer group,
nothing committed), because the load balancer may put any device's channel on any node, and a member's two devices
on two nodes. A node keeps no event bodies in memory: only a small index per member of (sequence number,
partition, offset) for recent events. On start it reads the log from the beginning (or --replay-s back) to rebuild
that index, then accepts its channels.

Channels drop (--drops a second: a phone moving from Wi-Fi to 4G, the app going to the background, a node being
replaced), half of them (--mid-payment) on members with a payment in flight, and come back --away seconds later
asking to resume after the last sequence number the device acknowledged (Propeller's ChannelEventAck). A separate
replay task, on its own connection, reads the missed events back from the log at the indexed offsets and sends
them in order, then the events that arrived meanwhile, before the channel goes live again. The read loops never wait on a replay.

Prints a line a second: backend-to-node latency (p50, p99, p99.9) with PASS or FAIL against --budget-ms, events
pushed to devices, and the reconnects recovered: events replayed from the log, payments that finished while the
device was away (each one a status screen that would otherwise have to poll), failures with their reason, CRED
coin balances, resend time, and gaps (must be 0).
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, EVENTS, FINAL, PARTITIONS, STREAM, Window, connect, devices_of, ensure_topics, load, \
    member, node_of


class Channel:
    __slots__ = ("k", "last", "state", "back_at", "pending", "t_back")

    def __init__(self, k):
        self.k, self.last, self.state, self.back_at, self.pending, self.t_back = k, 0, "off", 0.0, [], 0.0


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), EVENTS)
    P = PARTITIONS[EVENTS]
    start_ns = time.time_ns()
    rng = random.Random(a.name)

    index = defaultdict(lambda: deque(maxlen=a.index_depth))   # member -> recent (seq, partition, offset)
    channels, here = {}, defaultdict(list)    # device -> Channel; member -> its channels on this node
    for i in range(a.members):
        k = member(i)
        for d in devices_of(k):
            if node_of(d, a.nodes) == a.index:
                channels[d] = Channel(k)
                here[k].append(channels[d])
    up = []                                   # devices with a live channel, for picking drops
    active = deque(maxlen=2000)               # members with a live event in the last moments
    resends = asyncio.Queue()
    lat, warm, st, run = Window(), Window(), defaultdict(int), defaultdict(int)
    resend_ms = Window()
    caught = [None]
    n_up = [0]                                # channels live now

    def deliver(c, seq):
        # What the device sees: every event exactly once, in sequence order.
        if seq <= c.last:
            st["dups"] += 1
            return
        if seq != c.last + 1:
            st["gaps"] += 1                   # the app would have missed a status
        c.last = seq

    def apply(pid, ms, now):
        for m in ms:
            r = load(m.payload())
            k, seq = r["k"], r["seq"]
            idx = index[k]
            if idx and seq <= idx[-1][0]:
                run["repeats"] += 1            # at-least-once redelivery: already indexed
                continue
            idx.append((seq, pid, m.offset()))
            if caught[0] is None or r["ns"] < start_ns:
                warm.add((now - r["ns"]) / 1e6)
                run["replayed"] += 1           # rebuilding the index, not live
                continue
            lat.add((now - r["ns"]) / 1e6)
            st["read"] += 1
            if k in here:
                active.append(k)
            for c in here.get(k, ()):
                if c.state == "up":
                    deliver(c, seq)
                    st["pushed"] += 1
                    st["final_live"] += r["status"] in FINAL
                elif c.state == "replaying":
                    c.pending.append(seq)      # held until the replay is sent
                elif c.state == "away":
                    st["missed"] += 1          # the device is away; it will ask for these on reconnect

    async def read(pid):
        c = await connect(a.url)
        who, nxt = Consumer.Single(f"{me}-{pid}"), None
        first = PollingStrategy.Timestamp(int((time.time() - a.replay_s) * 1e6)) if a.replay_s else PollingStrategy.First()
        while True:
            ms = await c.poll_messages(STREAM, EVENTS, consumer=who, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else first)
            if ms:
                apply(pid, ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                   # a rebuild or a burst: read on at once
            await asyncio.sleep(a.poll_ms / 1000)

    async def replay():
        # Resend requests, one at a time, on their own connection. The index already holds the offset of every
        # missed event, so the replay fetches only those offsets (one short range at a time) and decodes only
        # those events; scanning everything in the partition since the device dropped would hold the event loop,
        # and so every read loop, for milliseconds.
        c = await connect(a.url)
        who = Consumer.Single(f"{me}-replay")
        while True:
            d, frm, upto = await resends.get()
            ch = channels[d]
            got = []
            want = [(q, p, o) for q, p, o in index[ch.k] if frm <= q <= upto]
            if not want or want[0][0] != frm:
                st["unrecoverable"] += 1       # older than the index: the app would need a full refresh
            else:
                pid = want[0][1]               # a member's events all live in one partition
                offs = [o for _, _, o in want]
                i = 0
                while i < len(offs):
                    off = offs[i]
                    j = i                      # fetch up to the last wanted offset within the next 64
                    while j + 1 < len(offs) and offs[j + 1] - off < 64:
                        j += 1
                    ms = await c.poll_messages(STREAM, EVENTS, consumer=who, partition_id=pid,
                                               count=offs[j] - off + 1, auto_commit=False,
                                               polling_strategy=PollingStrategy.Offset(off))
                    if not ms:
                        break
                    base = ms[0].offset()
                    for o in offs[i:j + 1]:
                        n = o - base
                        m = ms[n] if 0 <= n < len(ms) and ms[n].offset() == o else next(
                            (x for x in ms if x.offset() == o), None)
                        if m is not None:
                            r = load(m.payload())
                            if r["k"] == ch.k and (not got or r["seq"] > got[-1]["seq"]):
                                got.append(r)
                    i = j + 1
            for r in got:                       # resent in order
                deliver(ch, r["seq"])
                st["resent"] += 1
                st["finished"] += r["status"] in FINAL
                st["failed"] += r["status"] in ("FAILED", "PAYOUT_FAILED")
                st["coins"] += r["status"] == "COINS_BALANCE"
            for seq in sorted(ch.pending):      # then whatever arrived while replaying
                deliver(ch, seq)
            ch.pending, ch.state = [], "up"
            n_up[0] += 1
            up.append(d)
            st["recovered"] += 1
            resend_ms.add((time.monotonic() - ch.t_back) * 1e3)

    async def churn():
        # Channels drop and reconnect.
        away = []
        due = time.monotonic()
        while True:
            due += 0.1
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            if caught[0] is None:
                continue
            now = time.monotonic()
            for _ in range(int(a.drops / 10 + rng.random())):
                if len(up) < 2:
                    break
                if active and rng.random() < a.mid_payment:
                    # A member who just paid often leaves the app or changes network while the bank responds.
                    ch = next((c for c in here[rng.choice(active)] if c.state == "up"), None)
                    if ch is None:
                        continue
                    d = next(x for x in devices_of(ch.k) if channels.get(x) is ch)
                    # its entry in `up` goes stale and is skipped when picked (no O(n) remove)
                else:
                    i = rng.randrange(len(up))
                    up[i], up[-1] = up[-1], up[i]
                    d = up.pop()
                    ch = channels[d]
                    if ch.state != "up":
                        continue
                ch.state, ch.back_at = "away", now + rng.uniform(*a.away)
                n_up[0] -= 1
                away.append(d)
                st["dropped"] += 1
            still = []
            for d in away:
                ch = channels[d]
                if ch.back_at > now:
                    still.append(d)
                    continue
                st["reconnects"] += 1
                ch.t_back = now
                last = index[ch.k][-1][0] if index[ch.k] else 0
                if last <= ch.last:
                    ch.state = "up"             # nothing happened for this member while the device was away
                    n_up[0] += 1
                    up.append(d)
                    st["recovered"] += 1
                    resend_ms.add(0.0)
                    continue
                ch.state = "replaying"          # the reconnect asks to resume after ch.last
                resends.put_nowait((d, ch.last + 1, last))
            away = still

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                w = warm.flush()
                lat.flush()
                # Caught up only after a whole second in which every event read was inside the budget.
                if w["n"] and w["max"] < a.budget_ms and run["seconds"] >= 1:
                    caught[0] = time.monotonic()
                    for d, ch in channels.items():   # connect every channel at the member's current sequence
                        ch.last = index[ch.k][-1][0] if index[ch.k] else 0
                        ch.state = "up"
                    up.extend(channels)
                    n_up[0] = len(channels)
                    gc.freeze()                 # keep the rebuilt index out of the collector's way
                    print(f"[{a.name}] rebuilt the index from the log in {(time.time_ns() - start_ns) / 1e9:.2f}s: "
                          f"{len(index):,} members, {run['replayed']:,} events; {len(channels):,} device channels "
                          f"connected", flush=True)
                else:
                    print(f"[{a.name}] catching up: {run['replayed']:,} events read from the log, "
                          f"{len(index):,} members indexed", flush=True)
                run["seconds"] += 1
                st.clear()
                continue
            r, rs = lat.flush(), resend_ms.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>6,} events/s  backend-to-node p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  to devices {st['pushed']:,}/s "
                  f"(final statuses {st['final_live']:,})  reconnects {st['reconnects']} recovered {st['recovered']}  "
                  f"replayed from the log {st['resent']:,} (payments finished while away {st['finished']:,}, "
                  f"failures with reason {st['failed']:,}, coin balances {st['coins']:,})  "
                  f"resend p99 {rs['p99']:.0f}ms  gaps {st['gaps']}  dups {st['dups']}  "
                  f"unrecoverable {st['unrecoverable']}  channels up {n_up[0]:,} of {len(channels):,}", flush=True)
            st.clear()

    print(f"[{a.name}] Propeller node {a.index + 1} of {a.nodes}: channels for {len(channels):,} devices of "
          f"{len(here):,} members, reading {P} partitions", flush=True)
    await asyncio.gather(report(), churn(), replay(), *(read(p) for p in range(P)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="node-1")
    p.add_argument("--index", type=int, default=0, help="this node's number, from 0")
    p.add_argument("--nodes", type=int, default=2, help="Propeller nodes sharing the channels")
    p.add_argument("--members", type=int, default=500_000, help="same as payments.py")
    p.add_argument("--budget-ms", type=float, default=10, help="p99 budget for backend-to-node")
    p.add_argument("--drops", type=float, default=20, help="device channels dropped per second on this node")
    p.add_argument("--mid-payment", type=float, default=0.5,
                   help="share of drops that hit a member with a live event moments ago (a payment in flight)")
    p.add_argument("--away", type=float, nargs=2, default=(0.3, 3.0), metavar=("MIN", "MAX"),
                   help="seconds a dropped channel stays away")
    p.add_argument("--index-depth", type=int, default=256, help="recent events indexed per member")
    p.add_argument("--replay-s", type=float, default=0, help="rebuild from this many seconds back; 0 = whole log")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
