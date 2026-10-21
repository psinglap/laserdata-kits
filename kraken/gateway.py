"""A trading gateway (FIX or WebSocket): reads every execution report from the lane, delivers each one to the
account's session if it is logged on here, and replays missed reports from the log when a session reconnects.

Every gateway reads every partition directly, one connection per partition, with its own offsets (no consumer
group, nothing committed), because the load balancer may put any account's session on any gateway. A gateway
keeps no report bodies in memory: only a small index per account of (sequence number, partition, offset) for
its recent reports. On start it reads the log from the beginning (or --replay-s back) to rebuild that index,
then logs its sessions on.

Sessions drop at random (--drops a second) and come back --away seconds later asking to resume from the next
sequence number they expect, as a FIX client does with a lower-than-expected MsgSeqNum. A separate replay task,
on its own connection, reads the missed reports back from the log at the indexed offset and sends them in order
(PossDupFlag set), then the reports that arrived meanwhile, before the session goes live again. The read loops
never wait on a replay.

Prints a line a second: engine-to-gateway latency (p50, p99, p99.9) with PASS or FAIL against --budget-ms, reports
delivered to sessions, and the reconnects recovered: reports and fills replayed from the log, orders that closed
while the session was away (an open-orders snapshot alone would not show them), resend time, and gaps (must be 0).
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, PARTITIONS, REPORTS, STREAM, Window, account, connect, ensure_topics, gateway_of, load


class Session:
    __slots__ = ("last", "state", "back_at", "pending", "t_back")

    def __init__(self):
        self.last, self.state, self.back_at, self.pending, self.t_back = 0, "off", 0.0, [], 0.0


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), REPORTS)
    P = PARTITIONS[REPORTS]
    start_ns = time.time_ns()
    rng = random.Random(a.name)

    index = defaultdict(lambda: deque(maxlen=a.index_depth))   # account -> recent (seq, partition, offset)
    sessions = {k: Session() for k in (account(i) for i in range(a.accounts)) if gateway_of(k, a.gateways) == a.index}
    up = []                                   # accounts with a live session, for picking drops
    resends = asyncio.Queue()
    lat, warm, st, run = Window(), Window(), defaultdict(int), defaultdict(int)
    resend_ms = Window()
    caught = [None]

    def deliver(s, seq):
        # What the client sees: every report exactly once, in sequence order.
        if seq <= s.last:
            st["dups"] += 1
            return
        if seq != s.last + 1:
            st["gaps"] += 1                   # the client would see a sequence gap
        s.last = seq

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
            s = sessions.get(k)
            if s is None or s.state == "off":
                continue
            if s.state == "up":
                deliver(s, seq)
                st["delivered"] += 1
            elif s.state == "replaying":
                s.pending.append(seq)          # held until the replay is sent
            else:
                st["missed"] += 1              # the session is away; it will ask for these on reconnect

    async def read(pid):
        c = await connect(a.url)
        who, nxt = Consumer.Single(f"{me}-{pid}"), None
        first = PollingStrategy.Timestamp(int((time.time() - a.replay_s) * 1e6)) if a.replay_s else PollingStrategy.First()
        while True:
            ms = await c.poll_messages(STREAM, REPORTS, consumer=who, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else first)
            if ms:
                apply(pid, ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                   # a rebuild or a burst: read on at once
            await asyncio.sleep(a.poll_ms / 1000)

    async def replay():
        # Resend requests, one at a time, on their own connection: read the missed reports back from the log.
        c = await connect(a.url)
        who = Consumer.Single(f"{me}-replay")
        while True:
            k, frm, upto = await resends.get()
            s = sessions[k]
            got = []
            start = next(((p, o) for q, p, o in index[k] if q == frm), None)
            if start is None:
                st["unrecoverable"] += 1       # older than the index: the client would need a full resync
            else:
                pid, off = start
                while len(got) < upto - frm + 1:
                    ms = await c.poll_messages(STREAM, REPORTS, consumer=who, partition_id=pid, count=a.batch,
                                               auto_commit=False, polling_strategy=PollingStrategy.Offset(off))
                    if not ms:
                        break
                    for m in ms:
                        r = load(m.payload())
                        if r["k"] == k and frm <= r["seq"] <= upto and (not got or r["seq"] > got[-1]["seq"]):
                            got.append(r)
                    off = ms[-1].offset() + 1
            for r in got:                       # resent with PossDupFlag=Y, in order
                deliver(s, r["seq"])
                st["resent"] += 1
                st["fills"] += r["exec_type"] in ("trade", "filled")
                st["closed"] += r["order_status"] in ("filled", "canceled")
            for seq in sorted(s.pending):       # then whatever arrived while replaying
                deliver(s, seq)
            s.pending, s.state = [], "up"
            up.append(k)
            st["recovered"] += 1
            resend_ms.add((time.monotonic() - s.t_back) * 1e3)

    async def churn():
        # Sessions drop and reconnect: a client restart, a network blip, a gateway failover.
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
                i = rng.randrange(len(up))
                up[i], up[-1] = up[-1], up[i]
                k = up.pop()
                s = sessions[k]
                if s.state != "up":
                    continue
                s.state, s.back_at = "away", now + rng.uniform(*a.away)
                away.append(k)
                st["dropped"] += 1
            still = []
            for k in away:
                s = sessions[k]
                if s.back_at > now:
                    still.append(k)
                    continue
                st["reconnects"] += 1
                s.t_back = now
                last = index[k][-1][0] if index[k] else 0
                if last <= s.last:
                    s.state = "up"              # nothing happened on the account while it was away
                    up.append(k)
                    st["recovered"] += 1
                    resend_ms.add(0.0)
                    continue
                s.state = "replaying"           # logon asks to resume from s.last + 1
                resends.put_nowait((k, s.last + 1, last))
            away = still

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                w = warm.flush()
                fresh = lat.flush()
                # Caught up only after a whole second in which every report read was inside the budget.
                if w["n"] and w["max"] < a.budget_ms and run["seconds"] >= 1:
                    caught[0] = time.monotonic()
                    for k, s in sessions.items():   # log every session on at the account's current sequence
                        s.last = index[k][-1][0] if index[k] else 0
                        s.state = "up"
                    up.extend(sessions)
                    gc.freeze()                 # keep the rebuilt index out of the collector's way
                    print(f"[{a.name}] rebuilt the index from the log in {(time.time_ns() - start_ns) / 1e9:.2f}s: "
                          f"{len(index):,} accounts, {run['replayed']:,} reports; {len(sessions):,} sessions logged on",
                          flush=True)
                else:
                    print(f"[{a.name}] catching up: {run['replayed']:,} reports read from the log, "
                          f"{len(index):,} accounts indexed", flush=True)
                run["seconds"] += 1
                st.clear()
                continue
            r, rs = lat.flush(), resend_ms.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            n_up = len(up)
            print(f"[{a.name}] {r['n']:>6,} reports/s  engine-to-gateway p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  to sessions {st['delivered']:,}/s  "
                  f"reconnects {st['reconnects']} recovered {st['recovered']}  replayed from the log {st['resent']:,} "
                  f"(fills {st['fills']:,}, orders closed while away {st['closed']:,})  "
                  f"resend p99 {rs['p99']:.0f}ms  gaps {st['gaps']}  dups {st['dups']}  "
                  f"unrecoverable {st['unrecoverable']}  sessions up {n_up:,} of {len(sessions):,}", flush=True)
            st.clear()

    print(f"[{a.name}] gateway {a.index + 1} of {a.gateways}: sessions for {len(sessions):,} accounts, reading "
          f"{P} partitions", flush=True)
    await asyncio.gather(report(), churn(), replay(), *(read(p) for p in range(P)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="gw-1")
    p.add_argument("--index", type=int, default=0, help="this gateway's number, from 0")
    p.add_argument("--gateways", type=int, default=2, help="gateways sharing the sessions")
    p.add_argument("--accounts", type=int, default=200_000, help="same as engine.py")
    p.add_argument("--budget-ms", type=float, default=10, help="p99 budget for engine-to-gateway")
    p.add_argument("--drops", type=float, default=20, help="sessions dropped per second on this gateway")
    p.add_argument("--away", type=float, nargs=2, default=(0.3, 3.0), metavar=("MIN", "MAX"),
                   help="seconds a dropped session stays away")
    p.add_argument("--index-depth", type=int, default=256, help="recent reports indexed per account")
    p.add_argument("--replay-s", type=float, default=0, help="rebuild from this many seconds back; 0 = whole log")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
