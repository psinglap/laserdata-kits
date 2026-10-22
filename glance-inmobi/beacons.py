"""The beacon collector, with the phones that call it: impression and dismiss beacons in, the lane out.

Each second, --syncs phones sync. A sync uploads the beacons the phone has stored since its last one: 1-3
lock-screen ad impressions, and for some of them a dismiss (the user swiped the ad away). Usage is skewed:
a small share of heavy users unlock all day and make a large share of the syncs. Some phones were offline and
upload beacons they held for minutes; the lane cannot win that time back, so it must not add to it.

For every beacon the collector publishes the resulting total on user-ad-signals, keyed by user:
  * an impression: this user's impressions of the campaign in the cap window;
  * a dismiss: this user's dismisses of the advertiser in the window;
with a per-(collector, user) sequence and the time the collector received it.

A mobile round trip after the upload (--gap-ms, 80-400 ms by default) the phone asks for its next prefetch
refill. The request goes on prefetch-refills, to the partition of one ad server picked at random, as a load
balancer would. The ad server has to know this sync's beacons by then.
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

from common import (DEFAULT_URL, PARTITIONS, REFILLS, SIGNALS, STREAM, advertiser_of, campaign, cap_window,
                    connect, ensure_topics, msg, part, user)


async def main(a):
    me = f"{a.name}.{os.getpid()}"          # a restarted collector is a new writer, so its totals start again
    await ensure_topics(await connect(a.url), SIGNALS, REFILLS)
    lane, refill = await connect(a.url), await connect(a.url)     # one connection per producer
    P = PARTITIONS[SIGNALS]
    users = [user(i) for i in range(a.users)]
    upart = [part(u, P) for u in users]
    camps = [campaign(i) for i in range(a.campaigns)]
    advs = [advertiser_of(c) for c in camps]
    heavy_n = max(1, int(a.users * a.heavy_share))
    rng = random.Random(me)
    seq = defaultdict(int)                  # user -> this collector's sequence
    tot = defaultdict(dict)                 # window -> {(user, campaign or advertiser): running total}
    pending = []                            # (due ns, n, user, ad server) refill requests not yet sent
    st = defaultdict(int)
    gc.freeze()                             # keep start-up tables out of the collector's way
    t0 = due = time.monotonic()
    tick_s, owed, n_req, last_print = a.tick_ms / 1000, 0.0, 0, time.monotonic()
    while a.duration <= 0 or time.monotonic() - t0 < a.duration:
        t = time.monotonic() - t0
        x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
        owed += a.syncs * x * tick_s
        w = cap_window(a.window_s)
        tw = tot[w]
        lanes = defaultdict(list)
        now = time.time_ns()
        for _ in range(int(owed)):
            i = rng.randrange(heavy_n) if rng.random() < a.heavy_traffic else rng.randrange(a.users)
            u = users[i]
            held = rng.uniform(30, 900) if rng.random() < a.offline else 0.0   # seconds the phone held them
            for _ in range(rng.randint(1, 3)):
                j = int(a.campaigns * rng.random() ** 3)
                for k, kind in ((camps[j], "imp"), (advs[j], "dis")):
                    if kind == "dis" and rng.random() >= a.dismiss:
                        continue
                    n = tw[(u, k)] = tw.get((u, k), 0) + 1
                    seq[u] += 1
                    lanes[upart[i]].append(SendMessage(msg(s=me, u=u, t=kind, k=k, w=w, seq=seq[u], n=n, held=round(held),
                                                           ns=now)))
                    st[kind] += 1
            st["held"] += held > 0
            st["syncs"] += 1
            n_req += 1
            heapq.heappush(pending, (now + int(rng.uniform(a.gap_min_ms, a.gap_max_ms) * 1e6), n_req, u,
                                     rng.randrange(a.ad_servers)))
        owed -= int(owed)
        refills = defaultdict(list)
        while pending and pending[0][0] <= now:
            _, _, u, s = heapq.heappop(pending)
            refills[s].append(SendMessage(msg(u=u)))
            st["refills"] += 1
        # One batch per partition, all sent at once: a user's beacons stay in order, one round trip a tick.
        sends = [lane.send_messages(STREAM, SIGNALS, Partitioning.partition_id(p), ms) for p, ms in lanes.items()]
        sends += [refill.send_messages(STREAM, REFILLS, Partitioning.partition_id(s), ms) for s, ms in refills.items()]
        if sends:
            await asyncio.gather(*sends)
        if time.monotonic() - last_print >= 1:
            last_print += 1
            print(f"[{a.name}] {st['syncs']:>6,} syncs/s ({'peak' if x > 1 else 'steady'})  beacons: impressions "
                  f"{st['imp']:,}  dismisses {st['dis']:,}  refill requests {st['refills']:,}  "
                  f"from phones that were offline {100 * st['held'] / max(1, st['syncs']):.0f}%", flush=True)
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
    p.add_argument("--syncs", type=float, default=3_000, help="phone syncs a second (beacon upload + refill request)")
    p.add_argument("--users", type=int, default=500_000, help="users; same on every ad server")
    p.add_argument("--heavy-share", type=float, default=0.01, help="share of users who unlock all day")
    p.add_argument("--heavy-traffic", type=float, default=0.3, help="share of syncs that come from them")
    p.add_argument("--campaigns", type=int, default=40, help="campaigns running on the lock screen")
    p.add_argument("--dismiss", type=float, default=0.06, help="share of impressions the user dismisses")
    p.add_argument("--offline", type=float, default=0.25, help="share of syncs with beacons a phone held offline")
    p.add_argument("--window-s", type=float, default=60, help="cap window in seconds (a compressed day); same everywhere")
    p.add_argument("--ad-servers", type=int, default=3, help="ad servers taking refill requests (at most 8)")
    p.add_argument("--gap-min-ms", type=float, default=80, help="shortest wait from beacon upload to refill request")
    p.add_argument("--gap-max-ms", type=float, default=400, help="longest wait from beacon upload to refill request")
    p.add_argument("--peak", type=float, default=1, help="sync-rate multiplier during the peak (a sale's first hour)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    p.add_argument("--tick-ms", type=float, default=10, help="how often the collector sends")
    p.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(p.parse_args()))
