"""A Broker API event stream gateway (the server behind a partner's trade events stream): reads each of its
partners' logs and pushes every trade event to that partner, resumes a partner from the last event_id it
processed, and lets a slow partner fall behind in the log instead of dropping its events.

Each partner's stream reads that partner's topic directly, one connection per partition, with its own offsets (no
consumer group, nothing committed). The gateway keeps, per partner, the recent delivery order as
(event_id, partition, offset), so a reconnect that says since_id=<event_id> maps to an exact position in every
partition. No time window, no duplicates to filter.

Three things happen to partners during a run:
  * Disconnects (--drops a minute per partner): the partner's connection drops and the last few events it was sent
    never arrive (lost in flight). It comes back --away seconds later with since_id = the last event it processed.
    Events created while it was away are held in the log and counted apart, not as live latency.
  * A slow partner (--slow, from --slow-at for --slow-for seconds): it can only process --slow-frac of its event
    rate. Alpaca's SSE docs say the server then drops messages ("you are reading too slowly, dropped N messages").
    Here it falls behind in its own log, loses nothing, and catches up afterwards; other partners are unaffected.
  * Everyone else reads live.

Prints a line a second: OMS-to-gateway latency over live partners (p50, p99, p99.9) with PASS or FAIL against
--budget-ms, events pushed, resumes and the events redelivered after since_id, events held while away, the slow
partner's lag in events, and gaps, duplicates and drops (all must be 0).
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, PARTS, STREAM, Window, connect, ensure_topics, load, topic


class Partner:
    def __init__(self, code, slow):
        self.code, self.slow = code, slow
        self.state = "catching up"           # catching up -> up; away; slow -> draining -> up
        self.epoch = 0                       # bumped on every reconnect: a poll from before it is discarded
        self.nxt = {p: None for p in range(PARTS[code])}   # next offset to read per partition (None: from start)
        self.sent = deque(maxlen=200_000)    # delivery order: (event_id, partition, offset, account, seq)
        self.last = {}                       # what the partner has processed: account -> last seq
        self.live_ns = 1 << 62               # events created at or after this time count as live
        self.back_at = 0.0
        self.t_back = 0.0
        self.budget = 0.0                    # how many events a slow partner may still process this second
        self.lag = 0
        self.rewind = {}


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    codes = a.partners
    await ensure_topics(await connect(a.url), codes)
    rng = random.Random(a.name)
    partners = {c: Partner(c, c == a.slow) for c in codes}
    t0 = time.monotonic()
    lat, st, tot = Window(), defaultdict(int), defaultdict(int)
    resume_ms = Window()
    caught = [None]

    def process(pt, r):
        # What the partner sees: every event of an account exactly once, in sequence order.
        k, seq = r["k"], r["seq"]
        last = pt.last.get(k)
        if last is not None:
            if seq <= last:
                st["dups"] += 1
                return
            if seq != last + 1:
                st["gaps"] += 1
        pt.last[k] = seq

    def deliver(pt, pid, ms, now):
        warm = caught[0] is None             # before the gateway is caught up, every event read is replay
        for m in ms:
            r = load(m.payload())
            pt.sent.append((r["event_id"], pid, m.offset(), r["k"], r["seq"]))
            process(pt, r)
            age = (now - r["ns"]) / 1e6
            if warm:
                tot["warm_max"] = max(tot["warm_max"], age)
                tot["warm_n"] += 1
                st["held"] += 1
                continue
            if pt.state == "up" and r["ns"] >= pt.live_ns:
                lat.add(age)
                st["pushed"] += 1
                if pt.t_back and age < a.budget_ms:
                    resume_ms.add((time.monotonic() - pt.t_back) * 1e3)   # reconnect to first live event
                    pt.t_back = 0.0
                continue
            st["held"] += 1                    # read back from the log: after a reconnect, or a slow partner
            if pt.slow:
                st["slow_read"] += 1
            if pt.state == "draining" and age < a.budget_ms:
                pt.state, pt.live_ns = "up", now   # the slow partner is back at the head of its log
                tot["slow_catchup_s"] = time.monotonic() - tot["slow_end"]
                st["slow_caught_up"] = 1

    async def read(pt, pid):
        c = await connect(a.url)
        who = Consumer.Single(f"{me}-{pt.code}-{pid}")
        while True:
            if pt.state == "away":
                await asyncio.sleep(a.poll_ms / 1000)
                continue
            count = a.batch
            if pt.state == "slow":
                count = min(a.batch, int(pt.budget))
                if count <= 0:
                    await asyncio.sleep(a.poll_ms / 1000)
                    continue
            ep, nxt = pt.epoch, pt.nxt[pid]
            ms = await c.poll_messages(STREAM, topic(pt.code), consumer=who, partition_id=pid, count=count,
                                       auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ep != pt.epoch or pt.state == "away":
                continue                       # the connection dropped meanwhile: the resume sets the position
            if ms:
                deliver(pt, pid, ms, time.time_ns())
                pt.nxt[pid] = ms[-1].offset() + 1
                if pt.state == "slow":
                    pt.budget -= len(ms)
                if len(ms) == count and pt.state != "slow":
                    continue                   # behind: read on at once
            await asyncio.sleep(a.poll_ms / 1000)

    def drop(pt, now):
        # The connection drops; the last few events sent never reached the partner.
        lost = min(len(pt.sent), rng.randint(0, a.in_flight))
        pt.rewind = {}
        for _ in range(lost):
            _, pid, off, k, seq = pt.sent.pop()
            pt.last[k] = seq - 1               # the partner never processed it
            pt.rewind[pid] = off               # (only used if since_id is older than everything kept)
        pt.state, pt.back_at = "away", now + rng.uniform(*a.away)
        pt.epoch += 1
        st["drops"] += 1
        st["lost"] += lost

    def resume(pt, now):
        # The partner reconnects with since_id = the last event_id it processed (its own record, here the last
        # event that reached it). The gateway finds that event in its delivery order and sets every partition to
        # right after the last event from that partition at or before it.
        since_id = pt.sent[-1][0] if pt.sent else None
        found, seen = False, set()
        for eid, pid, off, _, _ in reversed(pt.sent):
            found = found or eid == since_id
            if found and pid not in seen:
                pt.nxt[pid] = off + 1
                seen.add(pid)
                if len(seen) == len(pt.nxt):
                    break
        if since_id is not None and not found:
            st["unrecoverable"] += 1           # older than the delivery index: would resume by time instead
        for pid, off in pt.rewind.items():
            if pid not in seen:
                pt.nxt[pid] = off              # nothing from this partition reached the partner before since_id
        pt.state, pt.live_ns, pt.t_back = "up", time.time_ns(), now
        pt.epoch += 1
        st["resumes"] += 1

    async def life():
        due = time.monotonic()
        while True:
            due += 0.1
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            if caught[0] is None:
                continue
            now = time.monotonic()
            el = now - t0
            for pt in partners.values():
                if pt.slow:
                    if pt.state == "up" and a.slow_at <= el < a.slow_at + a.slow_for:
                        pt.state = "slow"
                        tot["slow_start"] = now
                    elif pt.state == "slow" and el >= a.slow_at + a.slow_for:
                        pt.state = "draining"
                        tot["slow_end"] = now
                    if pt.state == "slow":
                        pt.budget = min(pt.budget + a.slow_rate / 10, max(1.0, a.slow_rate / 10))   # what it can process next
                    continue
                if pt.state == "up" and rng.random() < a.drops / 600:
                    drop(pt, now)
                elif pt.state == "away" and now >= pt.back_at:
                    resume(pt, now)

    async def lagging():
        # The slow partner's lag: events in its log it has not read yet (end offset per partition - next offset).
        c = await connect(a.url)
        while True:
            await asyncio.sleep(1)
            pt = partners.get(a.slow)
            if pt is None:
                continue
            t = await c.get_topic(STREAM, topic(pt.code))
            if t is None:
                continue
            behind = 0
            for i, p in enumerate(sorted(t.partitions, key=lambda x: x.id)):
                end = p.current_offset + 1 if p.messages_count else 0
                behind += max(0, end - (pt.nxt.get(i) or 0))
            pt.lag = behind
            tot["max_lag"] = max(tot["max_lag"], behind)

    gc_t = [0.0]

    def on_gc(phase, info):
        # The longest garbage-collector pause each second, printed so a slow second can be explained.
        if phase == "start":
            gc_t[0] = time.perf_counter()
        else:
            tot["gc_ms"] = max(tot["gc_ms"], (time.perf_counter() - gc_t[0]) * 1e3)

    gc.callbacks.append(on_gc)

    async def loop_lag():
        # How late the event loop wakes a 5 ms timer: a stalled loop delays every partner's read.
        due = time.monotonic()
        while True:
            due += 0.005
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            tot["loop_ms"] = max(tot["loop_ms"], (time.monotonic() - due) * 1e3)

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                lat.flush()
                held = st["held"]
                st.clear()
                # Caught up only after a whole second in which every event read was inside the budget.
                if tot["warm_s"] >= 1 and tot["warm_max"] < a.budget_ms and held:
                    caught[0] = time.monotonic()
                    now_ns = time.time_ns()
                    for pt in partners.values():
                        pt.state, pt.live_ns = "up", now_ns
                    gc.freeze()
                    print(f"[{a.name}] caught up with the logs in {caught[0] - t0:.2f}s: {tot['warm_n']:,} events "
                          f"read back; streaming live to {len(partners)} partners", flush=True)
                else:
                    print(f"[{a.name}] catching up: {tot['warm_n']:,} events read from the logs", flush=True)
                tot["warm_s"] += 1
                tot["warm_max"] = 0.0
                continue
            r, rs = lat.flush(), resume_ms.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            sp = partners.get(a.slow)
            slow = (f"  slow partner {sp.code}: {sp.state}, lag {sp.lag:,} events, read from the log {st['slow_read']:,}"
                    + (f", caught up {tot['slow_catchup_s']:.2f}s after it sped up" if st["slow_caught_up"] else "")
                    if sp else "")
            print(f"[{a.name}] {r['n']:>6,} events/s  OMS-to-gateway p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  to partners {st['pushed']:,}/s  "
                  f"disconnects {st['drops']} resumes {st['resumes']} (lost in flight {st['lost']}, "
                  f"resume-to-live p99 {rs['p99']:.0f}ms)  held in the log {st['held']:,}{slow}  "
                  f"gaps {st['gaps']}  dups {st['dups']}  unrecoverable {st['unrecoverable']}  "
                  f"loop late max {tot['loop_ms']:.0f}ms  gc max {tot['gc_ms']:.0f}ms", flush=True)
            st.clear()
            tot["loop_ms"] = tot["gc_ms"] = 0.0

    print(f"[{a.name}] gateway for {len(codes)} partners ({', '.join(codes)}), reading "
          f"{sum(PARTS[c] for c in codes)} partitions; slow partner: {a.slow or 'none'}", flush=True)
    await asyncio.gather(report(), life(), lagging(), loop_lag(),
                         *(read(pt, p) for pt in partners.values() for p in range(PARTS[pt.code])))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="gw-1")
    p.add_argument("--partners", nargs="+", default=["corr-01", "corr-02", "corr-03"])
    p.add_argument("--budget-ms", type=float, default=10, help="p99 budget for OMS-to-gateway")
    p.add_argument("--drops", type=float, default=6, help="disconnects per partner per minute")
    p.add_argument("--away", type=float, nargs=2, default=(0.3, 2.0), metavar=("MIN", "MAX"),
                   help="seconds a dropped partner stays away")
    p.add_argument("--in-flight", type=int, default=20, help="up to this many events lost in flight on a drop")
    p.add_argument("--slow", default="", help="the partner that reads slowly for a while")
    p.add_argument("--slow-at", type=float, default=20, help="seconds after the gateway starts")
    p.add_argument("--slow-for", type=float, default=15)
    p.add_argument("--slow-rate", type=float, default=30, help="events per second the slow partner can process")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=2000)
    asyncio.run(main(p.parse_args()))
