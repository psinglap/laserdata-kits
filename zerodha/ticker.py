"""A ticker node: reads every tick from the lane and keeps the latest quote per instrument.

Every node reads every partition with its own connection and its own offsets (no shared
consumer group), so adding a node never slows the others: each one gets every tick, in order
per instrument. Stands in for the ticker's fan-out by counting, for each tick, the packets
it would push to --users simulated users, each watching --watch instruments.

On start it rebuilds the latest quote for every instrument by replaying the last --replay-s
seconds of the log (one rolling snapshot from feed.py and the ticks since), then times live ticks. Prints a line a second:
  * feed-to-ticker latency (tick created at the feed handler -> applied here), with PASS or
    FAIL against --budget-ms;
  * shown after a newer tick existed: quotes pushed to users when the feed had already created
    a newer tick for that instrument (users saw a price that was already old);
  * gaps: a missing sequence number for an instrument (must stay 0).
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, SEGMENTS, STREAM, TICKS, Window, connect, instruments, popular, unpack


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, TICKS):
        print(f"[{a.name}] waiting for feed.py to create the tick lane…", flush=True)
        await asyncio.sleep(1)

    tokens = instruments(a.instruments)
    rng = random.Random(a.name)
    watchers = {}                                   # token -> users watching it
    for _ in range(a.users * a.watch):
        t = tokens[popular(rng, len(tokens))]
        watchers[t] = watchers.get(t, 0) + 1
    gc.freeze()                                     # keep startup tables out of the collector's way

    seen, quote = {}, {}                            # token -> last seq read; token -> [seq, price, created ns, shown ns]
    lat, st = Window(), dict(live=0, shown=0, old=0, gaps=0, packets=0, replayed=0)
    started, caught, live_parts = time.monotonic(), [None], set()   # caught[0]: when every partition reached the live edge
    live_seen, budget_ns = set(), a.budget_ms * 1_000_000

    def apply(pid, ms, now):
        # Read every tick (order and gap check, latency), then show only each instrument's newest
        # tick from this read, as a ticker pushes the latest quote rather than every intermediate one.
        newest = {}
        for m in ms:
            token, ltp, seq, ns = unpack(m.payload())
            last = seen.get(token)
            if last is not None:
                if seq <= last:
                    continue                        # a repeat (snapshot or at-least-once redelivery): skip it
                if seq > last + 1:
                    st["gaps"] += 1                 # a tick for this instrument never arrived
            seen[token] = seq
            newest[token] = (seq, ltp, ns)
            if seq == 1:
                continue                            # the startup snapshot is not a live tick
            if pid not in live_parts:
                if now - ns >= budget_ns:
                    st["replayed"] += 1
                    continue
                live_parts.add(pid)                 # this partition's replay is done: time its live ticks from here
                if caught[0] is None and len(live_parts) == len(SEGMENTS):
                    caught[0] = time.monotonic()
            lat.add((now - ns) / 1e6)
            st["live"] += 1
        for token, (seq, ltp, ns) in newest.items():
            prev = quote.get(token)
            if prev is not None and seq > 1 and pid in live_parts:
                st["shown"] += 1
                st["packets"] += watchers.get(token, 0)
                live_seen.add(token)
                if ns < prev[3]:
                    st["old"] += 1                  # this tick existed before the previous one was shown
            quote[token] = [seq, ltp, ns, now]
        if caught[0] is not None and not st.get("told"):
            st["told"] = 1
            span = f"the last {a.replay_s:g} s of the log" if a.replay_s else "the whole log"
            print(f"[{a.name}] rebuilt from {span} in {caught[0] - started:.2f}s: quotes for {len(quote):,} of "
                  f"{len(tokens):,} instruments, {st['replayed']:,} ticks replayed", flush=True)

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        # Start a rolling snapshot ago (every quote is in it), or at the start of the retained log.
        first = PollingStrategy.Timestamp(int((time.time() - a.replay_s) * 1e6)) if a.replay_s else PollingStrategy.First()
        while True:
            ms = await c.poll_messages(STREAM, TICKS, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else first)
            if ms:
                apply(pid, ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                        # more is waiting (a replay or a burst): read on at once
            await asyncio.sleep(a.poll_ms / 1000)   # the feed writes once per --tick-ms; wait for the next write

    async def report():
        shown = False
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] catching up: {st['replayed']:,} ticks replayed, quotes for {len(quote):,} instruments, "
                      f"{len(live_parts)} of {len(SEGMENTS)} segments live", flush=True)
                lat.flush()
                continue
            r, n = lat.flush(), st["live"]
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            old = 100 * st["old"] / st["shown"] if st["shown"] else 0.0
            print(f"[{a.name}] {n:>7,} ticks/s  feed-to-ticker p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  packets to users {st['packets']:,}/s  "
                  f"shown after a newer tick existed {old:.3f}%  gaps {st['gaps']}  quoted {len(quote):,}", flush=True)
            if not shown and time.monotonic() - caught[0] >= 10:
                shown = True
                print(f"[{a.name}] without the log: 10 s of live ticks alone would quote {len(live_seen):,} of "
                      f"{len(tokens):,} instruments ({100 * len(live_seen) / len(tokens):.0f}%)", flush=True)
            st.update(live=0, shown=0, old=0, packets=0)

    await asyncio.gather(report(), *(read(p) for p in range(len(SEGMENTS))))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="ticker-1")
    p.add_argument("--instruments", type=int, default=60_000, help="same as feed.py")
    p.add_argument("--users", type=int, default=50_000, help="simulated users on this node")
    p.add_argument("--watch", type=int, default=50, help="instruments in each user's market watch")
    p.add_argument("--budget-ms", type=float, default=10, help="p99 budget for feed-to-ticker")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--replay-s", type=float, default=75, help="rebuild from this many seconds back (> feed.py --snapshot-every); 0 = whole log")
    asyncio.run(main(p.parse_args()))
