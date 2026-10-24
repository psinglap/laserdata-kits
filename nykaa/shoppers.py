"""The ad event collector, with the shoppers who browse Nykaa: sponsored ad impressions and clicks in, the lane out.

Each second, --pages shoppers load a page with sponsored slots (search results, a category listing, a product
page). The page shows 1-3 sponsored ads, and the shopper clicks some of them (--ctr). Browsing is skewed: a small
share of heavy shoppers make a large share of page loads, and a few campaigns take most of the slots.

For every ad event the collector publishes the resulting count on ad-events, keyed by shopper:
  * an impression: this shopper's impressions of the campaign in the window;
  * a click: this shopper's clicks on the campaign in the window;
with a per-(collector, shopper) sequence and the time the collector received it.

A scroll or a tap later (--gap-min-ms to --gap-max-ms, 300-3,000 ms by default) the shopper's next page asks
for sponsored ads. The request goes on ad-requests, to the partition of one ad selector picked at random, as a
load balancer would. The ad selector has to know this page's impressions and clicks by then.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import (DEFAULT_URL, EVENTS, PARTITIONS, REQUESTS, STREAM, campaign, clicked, connect, ensure_topics,
                    msg, part, shopper, window)


async def main(a):
    me = f"{a.name}.{os.getpid()}"          # a restarted collector is a new writer, so its counts start again
    await ensure_topics(await connect(a.url), EVENTS, REQUESTS)
    lane, req = await connect(a.url), await connect(a.url)       # one connection per producer
    P = PARTITIONS[EVENTS]
    shoppers = [shopper(i) for i in range(a.shoppers)]
    spart = [part(s, P) for s in shoppers]
    camps = [campaign(i) for i in range(a.campaigns)]
    clicks = [clicked(c) for c in camps]
    heavy_n = max(1, int(a.shoppers * a.heavy_share))
    rng = random.Random(me)
    seq = defaultdict(int)                  # shopper -> this collector's sequence
    tot = defaultdict(dict)                 # window -> {(shopper, key): running count}
    pending = []                            # (due ns, n, shopper, ad selector) ad requests not yet sent
    st = defaultdict(int)
    gc.freeze()                             # keep start-up tables out of the collector's way
    t0 = due = time.monotonic()
    tick_s, owed, n_req, last_print = a.tick_ms / 1000, 0.0, 0, time.monotonic()
    while a.duration <= 0 or time.monotonic() - t0 < a.duration:
        t = time.monotonic() - t0
        x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
        owed += a.pages * x * tick_s
        w = window(a.window_s)
        tw = tot[w]
        lanes = defaultdict(list)
        now = time.time_ns()
        for _ in range(int(owed)):
            i = rng.randrange(heavy_n) if rng.random() < a.heavy_traffic else rng.randrange(a.shoppers)
            s = shoppers[i]
            for _ in range(rng.randint(1, 3)):
                j = int(a.campaigns * rng.random() ** 3)
                for k, kind in ((camps[j], "imp"), (clicks[j], "clk")):
                    if kind == "clk" and rng.random() >= a.ctr:
                        continue
                    n = tw[(s, k)] = tw.get((s, k), 0) + 1
                    seq[s] += 1
                    lanes[spart[i]].append(SendMessage(msg(c=me, u=s, t=kind, k=k, w=w, seq=seq[s], n=n, ns=now)))
                    st[kind] += 1
            st["pages"] += 1
            n_req += 1
            heapq.heappush(pending, (now + int(rng.uniform(a.gap_min_ms, a.gap_max_ms) * 1e6), n_req, s,
                                     rng.randrange(a.selectors)))
        owed -= int(owed)
        asks = defaultdict(list)
        while pending and pending[0][0] <= now:
            _, _, s, sel = heapq.heappop(pending)
            asks[sel].append(SendMessage(msg(u=s)))
            st["requests"] += 1
        # One batch per partition, all sent at once: a shopper's events stay in order, one round trip a tick.
        sends = [lane.send_messages(STREAM, EVENTS, Partitioning.partition_id(p), ms) for p, ms in lanes.items()]
        sends += [req.send_messages(STREAM, REQUESTS, Partitioning.partition_id(sel), ms) for sel, ms in asks.items()]
        if sends:
            await asyncio.gather(*sends)
        if time.monotonic() - last_print >= 1:
            last_print += 1
            print(f"[{a.name}] {st['pages']:>6,} pages/s ({'peak' if x > 1 else 'steady'})  ad events: impressions "
                  f"{st['imp']:,}  clicks {st['clk']:,}  ad requests {st['requests']:,}", flush=True)
            st.clear()
            for k in [k for k in tot if k < w - 1]:
                del tot[k]                  # forget windows that ended more than one window ago
        due += tick_s
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print(f"[{a.name}] done", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="collector")
    p.add_argument("--pages", type=float, default=3_000, help="page loads with sponsored slots a second")
    p.add_argument("--shoppers", type=int, default=500_000, help="shoppers browsing; same on every ad selector")
    p.add_argument("--heavy-share", type=float, default=0.01, help="share of shoppers who browse heavily")
    p.add_argument("--heavy-traffic", type=float, default=0.3, help="share of page loads that come from them")
    p.add_argument("--campaigns", type=int, default=40, help="sponsored campaigns running")
    p.add_argument("--ctr", type=float, default=0.05, help="share of impressions the shopper clicks")
    p.add_argument("--window-s", type=float, default=60, help="counting window in seconds (a compressed day); same everywhere")
    p.add_argument("--selectors", type=int, default=3, help="ad selectors taking ad requests (at most 8)")
    p.add_argument("--gap-min-ms", type=float, default=300, help="shortest wait from a page's ads to the next ad request")
    p.add_argument("--gap-max-ms", type=float, default=3000, help="longest wait from a page's ads to the next ad request")
    p.add_argument("--peak", type=float, default=1, help="page-rate multiplier during the peak (a sale's first hour)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    p.add_argument("--tick-ms", type=float, default=10, help="how often the collector sends")
    p.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(p.parse_args()))
