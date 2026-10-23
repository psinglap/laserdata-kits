"""The source of truth: a stand-in for the VB3 auction core, selling lots on every virtual sale lane at once.

Each sale lane sells one lot at a time. A lot opens at its high preliminary bid; members bid live in fixed
increments; every accepted bid restarts the lot's countdown, and when the countdown runs out with no new bid the
lot sells to the high bidder and the lane opens its next lot. That is the second bidding step as Copart's 10-K
describes it: "When bidding stops, a countdown is initiated. If no bids are received during the countdown or any
extensions, the vehicle sells to the highest bidder." Countdown length, increments and values here are
illustrative.

Every change of a lane goes to the lot-state topic, partitioned by lane, with a per-lane sequence number and the
resulting state, never only a delta: lot opened, bid accepted (new high bid, the high bidder's region, countdown
restarted), lot sold.

Live bids arrive on live-bids from the edge nodes (bid_edge.py), keyed by lane, read directly from every
partition. Each bid is judged against the lane's true state at that moment:
  * accepted: the bid is above the high bid on a lot still open;
  * outbid: the bid is not above the high bid (the member's screen showed an older high bid, or another bid of
    the same amount landed first);
  * after the hammer: the lot it was placed on has sold already.
Each bid also carries the bid the same member would have placed from a screen refreshed every second, and that
bid is judged against the same state, before the live bid is applied. A refreshed screen that still showed the
previous lot as sold while the next one was open counts as a missed lot.
"""
import argparse
import asyncio
import gc
import itertools
import random
import time
from collections import defaultdict

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (BIDS, DEFAULT_URL, LOT_STATE, PARTITIONS, REGIONS, STREAM, Window, connect, ensure_topics,
                    increment, lane_key, load, msg, part_of)

LOT, ST, HIGH, BY, ERV, DL, SEQ, BID, NEXT = range(9)


async def main(a):
    await ensure_topics(await connect(a.url), LOT_STATE, BIDS)
    lane_w, bid_reader = await connect(a.url), await connect(a.url)

    rng = random.Random(11)
    lanes = [lane_key(y, l) for y in range(a.yards) for l in range(a.lanes_per_yard)]
    state = {}                     # lane -> [lot, status, high bid, high bidder region, erv, deadline ns, seq, bid, next ns]
    outbox = []                    # (lane, message) waiting for the next tick's send
    lots = itertools.count(30_000_000)
    st, lat, sold_usd = defaultdict(int), Window(), [0]

    def emit(k):
        v = state[k]
        v[SEQ] += 1
        outbox.append((k, SendMessage(msg(k=k, lot=v[LOT], st=v[ST], high=v[HIGH], by=v[BY], erv=v[ERV], dl=v[DL],
                                          bid=v[BID], seq=v[SEQ]))))
        st["changes"] += 1

    def open_lot(k, now_ns):
        v = state.setdefault(k, [0, "", 0, "", 0, 0, 0, "", 0])
        erv = int(rng.lognormvariate(8.9, 0.6))                  # estimated retail value, about $7,000 typical
        v[LOT], v[ST], v[ERV] = f"{next(lots)}", "open", erv
        v[HIGH] = max(100, int(erv * rng.uniform(0.04, 0.12)) // 25 * 25)   # the high preliminary bid
        v[BY], v[BID] = rng.choice(REGIONS), ""
        v[DL] = now_ns + int(a.open_s * 1e9)                     # time for live bidding to start on the new lot
        emit(k)

    async def send(items):
        out = defaultdict(list)            # one batch per partition: a lane's changes stay in order in one partition
        for key, m in items:
            out[part_of(key)].append(m)
        await asyncio.gather(*(lane_w.send_messages(STREAM, LOT_STATE, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))

    def verdict(k, lot, amt):
        v = state.get(k)
        if v is None or v[LOT] != lot or v[ST] != "open":
            return "hammer"                # the lot it was placed on has sold
        return "ok" if amt > v[HIGH] else "outbid"

    def on_bid(m):
        b = load(m.payload())
        k = b["k"]
        v = state.get(k)
        if v is None:
            return
        now = time.time_ns()
        lat.add((now - b["ns"]) / 1e6)
        st["bids"] += 1
        # Judge both bids against the same state, before this bid is applied.
        live_v = verdict(k, b["lot"], b["amt"])
        if b.get("alt_lot"):
            alt_v = verdict(k, b["alt_lot"], b["alt"])
        else:                              # the refreshed screen showed no open lot on this lane
            alt_v = "missed" if v[ST] == "open" else "none"
        if live_v == "ok":
            v[HIGH], v[BY], v[BID] = b["amt"], b["r"], b["id"]
            v[DL] = now + int(a.countdown_s * 1e9)               # every accepted bid restarts the countdown
            emit(k)
        st[f"live_{live_v}"] += 1
        st[f"alt_{alt_v}"] += 1

    async def read_bids():
        # Every partition read directly, from start-up on, on its own connection.
        who = {p: Consumer.Single(f"auction-core-{p}") for p in range(PARTITIONS[BIDS])}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)
        nxt = {}
        while True:
            more = False
            for p in who:
                ms = await bid_reader.poll_messages(STREAM, BIDS, consumer=who[p], partition_id=p, count=1000,
                                                    auto_commit=False,
                                                    polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_bid(m)
                    more = more or len(ms) == 1000
            if not more:
                await asyncio.sleep(0.001)

    async def run_lanes():
        now = time.time_ns()
        for k in lanes:                    # every lane's first lot (seq 1, not timed as a live change)
            open_lot(k, now)
        items, outbox[:] = list(outbox), []
        for i in range(0, len(items), 5000):
            await send(items[i:i + 5000])
        print(f"[auction] sale started: {len(lanes):,} lanes across {a.yards} yards, countdown {a.countdown_s:g} s "
              f"after the last bid", flush=True)
        gc.freeze()                        # keep start-up tables out of the collector's way
        due = time.monotonic()
        while True:
            now = time.time_ns()
            for k in lanes:
                v = state[k]
                if v[ST] == "open" and now >= v[DL]:
                    v[ST] = "sold"         # no bid during the countdown: sold to the high bidder
                    v[NEXT] = now + int(a.gap_s * 1e9)
                    st["sold"] += 1
                    sold_usd[0] += v[HIGH]
                    emit(k)
                elif v[ST] == "sold" and now >= v[NEXT]:
                    open_lot(k, now)       # the next lot on the block
            changes, outbox[:] = list(outbox), []
            t_send = time.monotonic()
            if changes:
                await send(changes)
            st["send_ms"] = max(st["send_ms"], 1000 * (time.monotonic() - t_send))
            st["late_ms"] = max(st["late_ms"], 1000 * (t_send - due))   # how late this tick started
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            n = st["bids"]
            live_bad = st["live_outbid"] + st["live_hammer"]
            alt_bad = st["alt_outbid"] + st["alt_hammer"] + st["alt_missed"]
            print(f"[auction] {st['changes']:>6,} lane changes/s  live bids {n:,} accepted {st['live_ok']:,}  "
                  f"stale-screen bids {live_bad:,} (outbid {st['live_outbid']:,} after the hammer {st['live_hammer']:,})  "
                  f"a screen refreshed every {a.refresh_s:g}s would lose {alt_bad:,} (outbid {st['alt_outbid']:,} "
                  f"after the hammer {st['alt_hammer']:,} missed lot {st['alt_missed']:,})  "
                  f"lots sold {st['sold']:,} total ${sold_usd[0]:,}  bid-to-core p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  "
                  f"tick late max {st['late_ms']:.0f}ms  send max {st['send_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(run_lanes(), read_bids(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--yards", type=int, default=200, help="yards selling at once")
    p.add_argument("--lanes-per-yard", type=int, default=2, help="virtual sale lanes per yard (up to 26)")
    p.add_argument("--countdown-s", type=float, default=3, help="countdown after the last accepted bid (illustrative)")
    p.add_argument("--open-s", type=float, default=4, help="time for live bidding to start on a new lot")
    p.add_argument("--gap-s", type=float, default=1, help="pause between a sale and the next lot on the lane")
    p.add_argument("--tick-ms", type=float, default=5, help="how often lane changes are sent and countdowns checked")
    p.add_argument("--refresh-s", type=float, default=1, help="the comparison screen's refresh, as set on bid_edge.py (label only)")
    asyncio.run(main(p.parse_args()))
