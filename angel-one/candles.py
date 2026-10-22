"""A chart candle builder: reads every tick from the lane and keeps each scrip's live candle.

The candle math is Angel One's own, from "Charting at Angel One": high = max, low = min, open
from the earliest tick and close from the latest by timestamp, so a tick updates the candle as
an upsert in any order. Every builder reads every partition with its own connection and its own
offsets (no shared consumer group), so a second builder, for example a DR copy in another region,
never slows the first; each one gets every tick, in order per scrip.

On start it replays the log from the start of the current candle (--replay period) or from the
start of the retained log (--replay first), so a builder that restarts or a DR builder that
takes over rebuilds the open candles from the log. Prints a line a second:
  * tick-to-candle delay: the tick's capture timestamp metered against this builder's clock when
    the candle has been updated, the delay Angel One meters today, with PASS or FAIL against
    --budget-ms on the p99;
  * on the chart within 1 s: the share of ticks inside today's p99 (~1,000 ms);
  * store upserts: changed candles written per second if the builder upserts once per --flush-ms;
  * late: ticks that arrived for a candle already closed (must stay 0); gaps: a missing sequence
    number for a scrip (must stay 0).
And a line per closed candle: when it was final after the candle ended, and a digest of every
scrip's open, high, low and close, so two builders can be checked against each other.
"""
import argparse
import asyncio
import gc
import hashlib
import time

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, PARTITIONS, STREAM, TICKS, Window, connect, hhmmss, unpack


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, TICKS):
        print(f"[{a.name}] waiting for capture.py to create the candle lane…", flush=True)
        await asyncio.sleep(1)

    tf = int(a.tf_s * 1e9)
    started_ns = time.time_ns()
    replay_from = 0 if a.replay == "first" else started_ns - started_ns % tf
    first = PollingStrategy.First() if a.replay == "first" else PollingStrategy.Timestamp(replay_from // 1000)
    budget_ns = a.budget_ms * 1e6

    seen = {}                                       # scrip -> last seq applied
    open_c = {}                                     # candle start ns -> {scrip: [open, high, low, close, open ts, close ts]}
    edge = [0] * PARTITIONS                         # newest tick timestamp read on each partition
    closed_upto = [0]                               # every candle that ends at or before this is final
    dirty = set()
    lat = Window()
    st = dict(live=0, inside=0, late=0, gaps=0, upserts=0, replayed=0, cn=0, cmax=0.0)
    caught = [None]
    gc.freeze()                                     # keep start-up objects out of the collector's way

    def close(start):
        cs = open_c.pop(start)
        end = start + tf
        closed_upto[0] = end
        final_ms = (time.time_ns() - end) / 1e6
        h = hashlib.sha1()
        for s in sorted(cs):
            o, hi, lo, c = cs[s][:4]
            h.update(b"%d:%d:%d:%d:%d;" % (s, o, hi, lo, c))
        if start < replay_from:
            print(f"[{a.name}] candle {hhmmss(start)} closed: {len(cs):,} scrips, partial (the replay began mid-candle)",
                  flush=True)
        else:
            print(f"[{a.name}] candle {hhmmss(start)} closed: {len(cs):,} scrips, digest {h.hexdigest()[:12]}, "
                  f"final {final_ms:.1f} ms after it ended", flush=True)

    def apply(pid, ms, now):
        for m in ms:
            scrip, ts, price, _vol, seq = unpack(m.payload())
            last = seen.get(scrip)
            if last is not None:
                if seq <= last:
                    continue                        # a repeat (at-least-once redelivery): skip it
                if seq > last + 1:
                    st["gaps"] += 1                 # a tick for this scrip never arrived
            seen[scrip] = seq
            if ts < closed_upto[0]:
                st["late"] += 1                     # its candle was already final
                continue
            start = ts - ts % tf
            cs = open_c.get(start)
            if cs is None:
                cs = open_c[start] = {}
            c = cs.get(scrip)
            if c is None:
                cs[scrip] = [price, price, price, price, ts, ts]
            else:                                   # Angel One's upsert: order does not matter
                if price > c[1]:
                    c[1] = price
                if price < c[2]:
                    c[2] = price
                if ts < c[4]:
                    c[0], c[4] = price, ts
                if ts >= c[5]:
                    c[3], c[5] = price, ts
            dirty.add((start, scrip))
            d = now - ts
            if caught[0] is None:
                st["replayed"] += 1
                st["cn"] += 1
                st["cmax"] = max(st["cmax"], d)
                continue
            lat.add(d / 1e6)
            st["live"] += 1
            if d < 1e9:
                st["inside"] += 1
        if ts > edge[pid]:
            edge[pid] = ts
        # Each partition is in timestamp order, so once every partition has passed a candle's end
        # (plus --grace-ms for capture clocks that disagree), no more ticks can arrive for it.
        low = min(edge) - a.grace_ms * 1e6
        while open_c:
            start = min(open_c)
            if start + tf > low:
                break
            close(start)

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        while True:
            ms = await c.poll_messages(STREAM, TICKS, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else first)
            if ms:
                apply(pid, ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                        # more is waiting (a replay or a burst): read on at once
            await asyncio.sleep(a.poll_ms / 1000)

    async def flush():
        # The upserts a builder would send to the candle store: each changed candle once per flush.
        while True:
            await asyncio.sleep(a.flush_ms / 1000)
            st["upserts"] += len(dirty)
            dirty.clear()

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                # Caught up only after a whole second in which every tick applied was inside the budget.
                if st["cn"] and st["cmax"] < budget_ns:
                    caught[0] = time.monotonic()
                    print(f"[{a.name}] rebuilt the open candles from {hhmmss(replay_from) if replay_from else 'the start of the log'}"
                          f" in {(time.time_ns() - started_ns) / 1e9:.2f}s: {st['replayed']:,} ticks replayed, "
                          f"{sum(len(v) for v in open_c.values()):,} candles open; timing live ticks from here", flush=True)
                else:
                    print(f"[{a.name}] catching up: {st['replayed']:,} ticks replayed", flush=True)
                st.update(cn=0, cmax=0.0, upserts=0)
                lat.flush()
                continue
            r, n = lat.flush(), st["live"]
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            inside = 100 * st["inside"] / n if n else 100.0
            print(f"[{a.name}] {n:>7,} ticks/s  tick-to-candle p50 {r['p50']:5.2f}ms  p95 {r['p95']:5.2f}ms  "
                  f"p99 {r['p99']:5.2f}ms  p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"on the chart within 1 s {inside:.3f}%  store upserts {st['upserts']:,}/s  "
                  f"late {st['late']}  gaps {st['gaps']}", flush=True)
            st.update(live=0, inside=0, upserts=0)

    await asyncio.gather(report(), flush(), *(read(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="candles-1")
    p.add_argument("--tf-s", type=float, default=60, help="candle timeframe in seconds (1-minute candles; higher ones are built from them)")
    p.add_argument("--budget-ms", type=float, default=25, help="p99 budget for tick-to-candle")
    p.add_argument("--replay", choices=("period", "first"), default="period",
                   help="on start, replay from the start of the current candle, or from the start of the log")
    p.add_argument("--grace-ms", type=float, default=0, help="extra wait before a candle is final")
    p.add_argument("--flush-ms", type=float, default=100, help="how often changed candles are upserted to the store")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
