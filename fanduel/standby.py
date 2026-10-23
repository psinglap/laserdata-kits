"""The wallet replica at site nj-2: reads every mirrored event and keeps each account's balance and open bets.

This is the part of the second site that must be ready to take bets the moment traffic is rerouted to it. It reads
every partition at nj-2 with its own connection and offsets (no consumer group), applies each account's events in
sequence order, and keeps the newest balance and open bets of every account in memory. A repeat or an older event
never replaces a newer one.

On start it rebuilds from nj-2's log, then times live events. Prints a line a second:
  * accepted-to-replica latency (accepted at nj-1 -> mirrored -> applied here at nj-2), PASS or FAIL against
    --budget-ms;
  * at risk if nj-1 went dark now: events nj-1 had accepted that this replica did not hold yet, sampled once a
    second from nj-1's partition heads (needs --primary-url; the business number);
  * events that waited at nj-1 while no mirror ran, counted and timed apart, not judged as live latency;
  * gaps (an account's event never arrived) and repeats (one arrived twice); both must stay 0.
"""
import argparse
import asyncio
import gc
import json
import time

from apache_iggy import Consumer, PollingStrategy

from common import LANE, PARTITIONS, PRIMARY, PRIMARY_URL, SECOND, SECOND_URL, STATUS, STREAM, Window, connect, \
    heads, unpack


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, LANE):
        print(f"[{a.name}] waiting for the mirror to create the wallet lane at {SECOND}…", flush=True)
        await asyncio.sleep(1)
    primary = await connect(a.primary_url) if a.primary_url else None
    wallet = {}                                              # account -> [seq, balance, open bets]
    gc.freeze()

    lat, held_w = Window(), Window()
    slow = []                                                # (pid, offset, ms) over budget: judged at report time
    held_to = [0] * PARTITIONS                               # offsets below this waited at nj-1 for a mirror start
    st = dict(live=0, gaps=0, repeats=0, replayed=0, rebuilt=0, max_age=0.0, held=0, settled=0, stake=0)
    applied_to = [0] * PARTITIONS
    started, head, caught = time.monotonic(), set(), [None, False]

    def apply(pid, ms, now):
        for msg in ms:
            v = unpack(msg.payload())
            acct, seq = v["a"], v["s"]
            w = wallet.get(acct)
            last = w[0] if w else 0
            if seq <= last:
                st["repeats"] += 1                           # never apply an older or repeated event
                continue
            if seq > last + 1 and caught[1]:
                st["gaps"] += 1                              # an event of this account never arrived
            wallet[acct] = [seq, v["bal"], v["o"]]
            age = (now - v["ns"]) / 1e6
            if not caught[1]:
                st["replayed"] += 1
                st["rebuilt"] += 1
                st["max_age"] = max(st["max_age"], age)
                continue
            if v["t"] == "bet.placed":
                st["stake"] += v["amt"]
            if age < a.budget_ms:
                lat.add(age)
                st["live"] += 1
            else:
                slow.append((pid, msg.offset(), age))       # live, or waited for a mirror start? decided below
        applied_to[pid] = ms[-1].offset() + 1

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        while True:
            ms = await c.poll_messages(STREAM, LANE, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else PollingStrategy.First())
            if ms:
                apply(pid, ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                                # more is waiting (the rebuild or a burst): read on at once
            if pid not in head:
                head.add(pid)                               # this partition's replay reached the head of the log
                if len(head) == PARTITIONS:
                    caught[0] = time.monotonic()
                    if not st["rebuilt"]:
                        print(f"[{a.name}] started on an empty lane at {SECOND}; timing every event from the first",
                              flush=True)
                    else:
                        print(f"[{a.name}] rebuilt from {SECOND}'s log in {caught[0] - started:.2f}s: balance and open bets "
                              f"of {len(wallet):,} accounts, {st['rebuilt']:,} events replayed; without the log "
                              f"this replica would start empty and copy every wallet from {PRIMARY}", flush=True)
            await asyncio.sleep(a.poll_ms / 1000)

    async def status():
        # Each mirror start says which offsets had already waited at nj-1; offsets are identical at both sites.
        c = await connect(a.url)
        await asyncio.sleep(0)
        me, nxt = Consumer.Single(f"{a.name}-status"), 0
        while True:
            if await c.get_topic(STREAM, STATUS):
                ms = await c.poll_messages(STREAM, STATUS, consumer=me, partition_id=0, count=100, auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt))
                for m in ms:
                    s = json.loads(m.payload())
                    for p in range(PARTITIONS):
                        held_to[p] = max(held_to[p], s["held_to"][p])
                    nxt = m.offset() + 1
            await asyncio.sleep(0.05)

    async def report():
        while True:
            await asyncio.sleep(1)
            risk = None
            if primary is not None:
                try:
                    h = await heads(primary)
                    risk = sum(max(0, x - y) for x, y in zip(h, applied_to))
                except RuntimeError:
                    risk = None                              # nj-1 unreachable: that is what failover is for
            if not caught[1]:
                # Live once the log is replayed and a whole second of events arrived inside the budget;
                # partitions interleave, so one fresh event alone doesn't mean the replay is finished.
                if caught[0] is not None and st["replayed"] and st["max_age"] < a.budget_ms:
                    caught[1] = True
                print(f"[{a.name}] catching up: {st['replayed']:,} events replayed this second, "
                      f"{len(head)} of {PARTITIONS} partitions at the head of {SECOND}'s log", flush=True)
                st.update(replayed=0, max_age=0.0)
                lat.flush()
                continue
            late = 0
            for pid, off, ms in slow:
                if off < held_to[pid]:
                    held_w.add(ms)                           # waited at nj-1 while no mirror ran: not live latency
                    st["held"] += 1
                else:
                    lat.add(ms)
                    st["live"] += 1
                    late += 1
            slow.clear()
            r, n, hw = lat.flush(), st["live"], held_w.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            risk_s = f"{risk:,} events" if risk is not None else "n/a"
            held_s = (f"held at {PRIMARY} for a mirror start {st['held']:,} (max {hw['max']:.0f}ms)" if st["held"]
                      else f"held at {PRIMARY} for a mirror start 0")
            print(f"[{a.name}] {n:>5,} events/s  accepted-to-replica p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  at risk if {PRIMARY} went dark now {risk_s}  "
                  f"stake placed ${st['stake'] / 100:,.0f}/s  {held_s}  accounts {len(wallet):,}  "
                  f"gaps {st['gaps']}  repeats {st['repeats']}", flush=True)
            st.update(live=0, held=0, stake=0)

    await asyncio.gather(report(), status(), *(read(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=SECOND_URL, help="the broker at the second in-state site (nj-2)")
    p.add_argument("--primary-url", default=PRIMARY_URL, help="nj-1, read for its partition heads only ('' to skip)")
    p.add_argument("--name", default="replica-1")
    p.add_argument("--budget-ms", type=float, default=50, help="p99 budget, accepted at nj-1 to applied at nj-2")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
