"""A forecast reader: Rider Pricing (--role pricing) or Driver Earnings (--role earnings).

Each process reads every partition of geohash-forecasts directly (its own offsets, no consumer group, nothing
committed) on its own connection. Chunks of a region's frame are staged; the region switches to the new frame
only when every chunk and the seal are in, so a price or a bonus zone never mixes two forecast runs. On start the
reader rebuilds the latest frame of every region from the last --rebuild-s seconds of the log, before it prices
a single ride, the way a reader comes back after a deploy.

Rider Pricing quotes rides (--quotes-per-s) in random regions and cells from the region's current frame. For
comparison it also keeps a copy of every region's frame refreshed every --poll-s seconds, the way a service that
polls for forecasts would see them. Each quote is judged a few seconds later against the same truth: the newest
frame the forecaster had sealed for that region when the quote was made. A quote priced on an older frame than
that was priced on a superseded forecast; the live lane and the polled copy are judged on the same quotes.

Driver Earnings counts the cells where the next 15 minutes of forecast demand runs ahead of supply (the cells a
bonus heatmap would light up), from each region's current frame.

Prints, every second: chunk-to-reader latency (p50, p99, p99.9) with PASS or FAIL against --budget-ms, how long
after the seal each frame was ready at this reader, and the role's business numbers.
"""
import argparse
import asyncio
import gc
import json
import os
import random
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, FORECASTS, PARTITIONS, STREAM, Window, connect, ensure_topics, load


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), FORECASTS)
    reader = await connect(a.url)
    P = PARTITIONS[FORECASTS]
    start_ns = time.time_ns()
    rng = random.Random(os.getpid())

    live = {}                             # region -> (frame, cells)   the frame every decision uses
    staging = defaultdict(dict)           # region -> frame -> {"chunks": {i: cells}, "of": n or None}
    sealed = defaultdict(dict)            # region -> frame -> seal creation ns (the truth quotes are judged on)
    polled = [{}]                         # region -> frame, refreshed every --poll-s (the comparison copy)
    quotes = deque()                      # (quote ns, region, live frame, polled frame)
    lat, ready, st, run = Window(), Window(), defaultdict(int), defaultdict(int)
    caught = [None]

    def complete(r, f, now):
        s = staging[r][f]
        if s["of"] is None or len(s["chunks"]) < s["of"]:
            return
        # cells stay raw bytes until the whole frame is in; parsed once, here
        cells = json.loads(b"[" + b",".join(s["chunks"][i][1:-1] for i in range(s["of"])) + b"]")
        live[r] = (f, cells)
        for g in [g for g in staging[r] if g <= f]:
            del staging[r][g]
        if caught[0] is not None and s["ns"] >= start_ns:
            ready.add((now - s["ns"]) / 1e6)      # seal created -> frame usable at this reader
            st["frames"] += 1
            if s.get("ev"):
                st["event_frames"] += 1
        if a.role == "earnings":
            # cells where demand over the next 15 minutes runs ahead of supply: a bonus heatmap would light them up
            st["zones"] += sum(1 for _, d, sp in cells if d[0] + d[1] + d[2] > 1.3 * (sp[0] + sp[1] + sp[2]))

    def apply(c, raw, now):
        r, f = c["r"], c["f"]
        if r in live and f <= live[r][0]:
            return                            # an older or repeated frame: the newest frame wins
        s = staging[r].setdefault(f, {"chunks": {}, "of": None, "ns": 0})
        if c["t"] == "c":
            s["chunks"][c["i"]] = raw
        else:
            s["of"], s["ns"], s["ev"] = c["of"], c["ns"], c["ev"]
            sealed[r][f] = c["ns"]
        complete(r, f, now)

    async def read_lane():
        who = {p: Consumer.Single(f"{me}-{p}") for p in range(P)}
        since = PollingStrategy.Timestamp(max(0, start_ns // 1000 - int(a.rebuild_s * 1e6)))   # microseconds
        nxt, done = {}, set()
        last = time.monotonic()
        while True:
            now_m = time.monotonic()
            st["gap_ms"] = max(st["gap_ms"], 1000 * (now_m - last))    # longest pass over all partitions
            last, more, got = now_m, False, []
            for p in range(P):
                ms = await reader.poll_messages(STREAM, FORECASTS, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        head, _, raw = m.payload().partition(b"\n")      # small header, then the cells
                        c = load(head)
                        if caught[0] is None:
                            run["replayed"] += 1
                        elif c["ns"] >= start_ns:           # created after this reader started: live, not the rebuild
                            lat.add((now - c["ns"]) / 1e6)
                            st["msgs"] += 1
                        got.append((c, raw))
                if len(ms) == a.batch:
                    more = True             # a rebuild or a burst: read on at once
                elif p not in done:
                    done.add(p)             # this partition has been read to the end of the log once
            # Read every partition first, then apply: a region's frame never waits on another partition's parsing.
            now = time.time_ns()
            for c, raw in got:
                apply(c, raw, now)
            if caught[0] is None and len(done) == P:
                caught[0] = time.monotonic()
                polled[0] = {r: v[0] for r, v in live.items()}
                gc.freeze()                 # keep the rebuilt state out of the collector's way
                print(f"[{a.name}] rebuilt the latest frame of {len(live):,} regions from the log in "
                      f"{(now - start_ns) / 1e9:.2f}s ({run['replayed']:,} messages replayed)", flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def refresh_copy():
        while caught[0] is None:
            await asyncio.sleep(0.05)
        due = time.monotonic()
        while True:
            due += a.poll_s
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            polled[0] = {r: v[0] for r, v in live.items()}

    async def price_rides():
        while caught[0] is None or not live:
            await asyncio.sleep(0.05)
        tick, owed, due = 0.01, 0.0, time.monotonic()
        regions = sorted(live)
        while True:
            owed += a.quotes_per_s * tick
            n, now = int(owed), time.time_ns()
            owed -= n
            for _ in range(n):
                r = rng.choice(regions)
                v = live.get(r)
                if v is None:
                    continue
                f, cells = v
                _, d, s = cells[rng.randrange(len(cells))]
                st["mult_sum"] += min(3.0, max(1.0, 1 + 0.5 * (d[0] - s[0]) / max(s[0], 0.5)))  # illustrative
                st["quotes"] += 1
                quotes.append((now, r, f, polled[0].get(r, 0)))
            due += tick
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    def judge():
        # Judge quotes made more than --judge-after-s ago: by then every seal created before them has arrived.
        cutoff = time.time_ns() - int(a.judge_after_s * 1e9)
        while quotes and quotes[0][0] < cutoff:
            q, r, f_live, f_poll = quotes.popleft()
            truth = max((g for g, ns in sealed[r].items() if ns <= q), default=0)
            st["judged"] += 1
            st["stale_live"] += f_live < truth
            st["stale_poll"] += f_poll < truth
        for r, fs in sealed.items():
            if len(fs) > 64:
                for g in sorted(fs)[:-64]:
                    del fs[g]

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding from the log: {run['replayed']:,} messages so far", flush=True)
                lat.flush()
                ready.flush()
                st.clear()
                continue
            r, fr = lat.flush(), ready.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            line = (f"[{a.name}] {st['msgs']:>5,} forecast msgs/s  chunk-to-{a.role} p50 {r['p50']:5.1f}ms  "
                    f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                    f"frames ready {st['frames']:,} (seal-to-ready p99 {fr['p99']:.1f}ms)")
            if a.role == "pricing":
                judge()
                j = max(st["judged"], 1)
                avg = st["mult_sum"] / max(st["quotes"], 1)
                line += (f"  quotes {st['quotes']:,} (avg multiplier {avg:.2f})  judged {st['judged']:,}: "
                         f"on a superseded forecast, live lane {st['stale_live']:,} ({100 * st['stale_live'] / j:.2f}%)"
                         f"  {a.poll_s:g} s poll {st['stale_poll']:,} ({100 * st['stale_poll'] / j:.2f}%)")
            else:
                line += f"  bonus-zone cells {st['zones']:,}"
            if st["event_frames"]:
                line += f"  concert frames {st['event_frames']}"
            print(line + f"  read pass max {st['gap_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(read_lane(), refresh_copy(), report(), *([price_rides()] if a.role == "pricing" else []))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--role", choices=("pricing", "earnings"), default="pricing")
    p.add_argument("--name", default=None, help="name in the output (default: rider-pricing or driver-earnings)")
    p.add_argument("--quotes-per-s", type=float, default=500, help="ride quotes priced per second (pricing)")
    p.add_argument("--poll-s", type=float, default=1, help="refresh period of the comparison copy")
    p.add_argument("--judge-after-s", type=float, default=3, help="judge a quote this long after it was made")
    p.add_argument("--budget-ms", type=float, default=50, help="chunk-to-reader p99 budget")
    p.add_argument("--rebuild-s", type=float, default=15, help="on start, rebuild from this many seconds of the log")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=2000)
    args = p.parse_args()
    args.name = args.name or ("rider-pricing" if args.role == "pricing" else "driver-earnings")
    asyncio.run(main(args))
