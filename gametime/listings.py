"""The source of truth: every seller listing on every event, the fans buying Zone Deals, and checkout's check.

Each listing belongs to one zone of one event's venue map. Every change goes to the zone-listings topic,
partitioned by event:zone, with a per-listing sequence number and the resulting state, never only a delta:
  * sold: the seller's tickets sold through another channel (sellers list the same tickets elsewhere too);
  * auto: Gametime's automatic pricing moved the price of a seller who opted in, on how the market is moving and
    the time left to the event, never under the seller's minimum price;
  * seller: the seller repriced by hand (never under their own minimum);
  * delisted, listed: the seller pulled the listing, or listed or added tickets;
  * zone_sold, zone_repick: checkout processed a Zone Deals pick on this listing.

It also plays the fans: Zone Deals orders go to zone-orders, keyed by event:zone. Each order carries the tickets
wanted and the Zone Deal price per ticket the fan paid. A picker (zone_picker.py) picks a listing from its pool
and sends the pick back. This script checks each pick against the listing's true state at that moment: a pick on
a listing without enough tickets left, or priced above what the fan paid, is a re-pick, and checkout has to pick
again (or absorb the difference). Each pick also carries the listing the same picker would have chosen from a
pool refreshed every few seconds, and that pick is checked against the same state.

--final-hour runs the last-minute window of one event: at --final-at, automatic pricing drops every opted-in
listing of the hottest event, its Zone Deal prices drop, its listing changes run at --hot-changes a second, and
its Zone Deals orders jump --final-hour times.
"""
import argparse
import asyncio
import gc
import itertools
import random
import time
from collections import defaultdict

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, LISTINGS, ORDERS, PARTITIONS, PICKS, STREAM, ZONES, Window, connect,
                    ensure_topics, load, msg, part_of, zone_key)

EV, Z, QTY, PRICE, FLOOR, AUTO, SEQ, ZQ = range(8)


async def main(a):
    await ensure_topics(await connect(a.url), LISTINGS, ORDERS, PICKS)
    lane, order_w, pick_reader = await connect(a.url), await connect(a.url), await connect(a.url)

    rng = random.Random(7)
    zones = ZONES[:a.zones]
    weight = [1.0 / (i + 1) ** 0.9 for i in range(a.events)]      # a few events draw most of the fans
    lst = {}                       # listing -> [event, zone key, qty, price, floor, auto, seq, zone qty processed]
    by_zone = defaultdict(list)    # zone key -> its listings
    by_event = defaultdict(list)   # event -> its listings
    deal = {}                      # zone key -> Zone Deal price per ticket
    keys = []
    outbox = []                    # (zone key, message) waiting for the next tick's send
    ids = itertools.count(1)
    st, lat, run = defaultdict(int), Window(), {"final": False, "dropped": False}
    pending = {}                   # order id -> (created ns, zone key, qty, paid)

    def emit(k, kind):
        v = lst[k]
        v[SEQ] += 1
        outbox.append((v[Z], SendMessage(msg(k=k, z=v[Z], seq=v[SEQ], qty=v[QTY], price=v[PRICE], floor=v[FLOOR],
                                             auto=v[AUTO], zq=v[ZQ], kind=kind))))
        st["changes"] += 1

    # The market at start: every event's zones, each with its sellers' listings (prices illustrative, USD).
    for e in range(a.events):
        base = rng.uniform(40, 260)
        for i, zn in enumerate(zones):
            z = zone_key(e, zn)
            mid = base * (1.0 + 0.35 * (len(zones) - i) / len(zones)) * (2.2 if zn in ("CLUB", "FLOOR") else 1.0)
            prices = []
            for _ in range(a.listings * (a.hot_x if e == 0 else 1)):    # the hottest event is a bigger venue
                k = f"L{next(ids):08d}"
                price = round(mid * rng.uniform(0.75, 1.4))
                lst[k] = [e, z, rng.choice((0, 2, 2, 2, 4, 4, 6, 8)), price, round(price * rng.uniform(0.55, 0.85)),
                          rng.random() < a.auto_share, 0, 0]
                by_zone[z].append(k)
                by_event[e].append(k)
                keys.append(k)
                prices.append(price)
            deal[z] = sorted(prices)[int(len(prices) * 0.35)]     # the Zone Deal price: a cheap third of the zone

    async def send(writer, topic, items):
        out = defaultdict(list)            # one batch per partition: a zone's messages stay in order in one partition
        for key, m in items:
            out[part_of(key, PARTITIONS[topic])].append(m)
        await asyncio.gather(*(writer.send_messages(STREAM, topic, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))

    async def snapshot():
        # Every listing's starting state (seq 1, not timed as a live change), like a full load of the market.
        for k in keys:
            emit(k, "snapshot")
        items, outbox[:] = list(outbox), []
        for i in range(0, len(items), 5000):
            await send(lane, LISTINGS, items[i:i + 5000])
        print(f"[listings] snapshot: {len(lst):,} listings, {sum(v[QTY] for v in lst.values()):,} tickets, "
              f"{a.events:,} events x {len(zones)} zones", flush=True)

    def one_change(k):
        v = lst[k]
        r = rng.random()
        if r < 0.30 and v[QTY] > 0:                        # sold through another channel
            v[QTY] = max(0, v[QTY] - rng.choice((1, 2, 2, 4)))
            return emit(k, "sold")
        if r < 0.60 and v[AUTO]:                           # automatic pricing: mostly down as the event nears
            f = rng.uniform(0.90, 0.98) if rng.random() < 0.7 else rng.uniform(1.02, 1.08)
            v[PRICE] = max(v[FLOOR], round(v[PRICE] * f))
            return emit(k, "auto")
        if r < 0.72:                                       # the seller reprices by hand
            v[PRICE] = max(v[FLOOR], round(v[PRICE] * rng.choice((0.9, 0.95, 1.05, 1.1))))
            return emit(k, "seller")
        if r < 0.82 and v[QTY] > 0:                        # the seller pulls the listing
            v[QTY] = 0
            return emit(k, "delisted")
        v[QTY] += rng.choice((2, 2, 4, 6))                 # the seller lists or adds tickets
        emit(k, "listed")

    def final_hour_drop(e):
        # The last-minute window: automatic pricing drops every opted-in listing of the event, and its Zone Deals.
        n = 0
        for k in by_event[e]:
            v = lst[k]
            if v[AUTO]:
                v[PRICE] = max(v[FLOOR], round(v[PRICE] * rng.uniform(0.78, 0.9)))
                emit(k, "auto")
                n += 1
        for zn in zones:
            deal[zone_key(e, zn)] = round(deal[zone_key(e, zn)] * 0.85)
        run["dropped"] = True
        print(f"[listings] final hour on E{e:05d}: automatic pricing dropped {n:,} listings, Zone Deals down 15%, "
              f"orders x{a.final_hour:g} and {a.hot_changes:g} changes/s on it for {a.final_for:g} s", flush=True)

    def shows_deal(z, q):
        # The app offers a zone's Zone Deal only while some listing in it has the tickets at or under the deal price.
        d = deal[z]
        return any(lst[k][QTY] >= q and lst[k][PRICE] <= d for k in by_zone[z])

    def tick_fans(n_orders, n_hot, now_ns):
        orders = []
        for j in range(n_orders + n_hot):
            e = 0 if j >= n_orders else rng.choices(range(a.events), weight)[0]
            q = rng.choice((1, 2, 2, 2, 3, 4))
            for _ in range(3):                 # a fan looks at up to three zones for a deal on their event
                z = zone_key(e, zones[rng.randrange(len(zones))])
                if shows_deal(z, q):
                    break
            else:
                st["no_deal"] += 1             # no Zone Deal on this event for that many tickets right now
                continue
            oid = f"o{next(ids)}"
            pending[oid] = (now_ns, z, q, deal[z])
            orders.append((z, SendMessage(msg(id=oid, z=z, qty=q, paid=deal[z], ns=now_ns))))
            st["orders"] += 1
        return orders

    async def run_market():
        await snapshot()
        gc.freeze()                        # keep start-up tables out of the collector's way
        t0 = due = time.monotonic()
        owed = defaultdict(float)
        while True:
            t = time.monotonic() - t0
            final = a.final_hour > 1 and a.final_at <= t < a.final_at + a.final_for
            if final and not run["dropped"]:
                final_hour_drop(0)
            run["final"] = final
            owed["c"] += a.rate / 100
            owed["o"] += a.orders / 100
            owed["h"] += (a.final_hour - 1) * a.orders / 100 if final else 0
            owed["hc"] += a.hot_changes / 100 if final else 0
            n = {x: int(v) for x, v in owed.items()}
            for x in n:
                owed[x] -= n[x]
            for _ in range(n["c"]):
                one_change(keys[rng.randrange(len(keys))])
            hot = by_event[0]
            for _ in range(n["hc"]):
                one_change(hot[rng.randrange(len(hot))])
            now_ns = time.time_ns()
            orders = tick_fans(n["o"], n["h"], now_ns)
            changes, outbox[:] = list(outbox), []
            # The tick's listing changes and orders go out together, each writer on its own connection.
            t_send = time.monotonic()
            await asyncio.gather(send(lane, LISTINGS, changes), send(order_w, ORDERS, orders))
            st["send_ms"] = max(st["send_ms"], 1000 * (time.monotonic() - t_send))
            st["late_ms"] = max(st["late_ms"], 1000 * (t_send - due))   # how late this tick started
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    def verdict(k, q, paid):
        v = lst.get(k)
        if v is None:
            return "none"
        if v[QTY] < q:
            return "gone"
        return "over" if v[PRICE] > paid else "ok"

    def on_pick(m):
        pk = load(m.payload())
        o = pending.pop(pk["id"], None)
        if o is None:
            return                         # picked twice (at-least-once) or already timed out
        lat.add((time.time_ns() - o[0]) / 1e6)
        st["picked"] += 1
        _, _, q, paid = o
        # Judge both picks against the same state, before this pick's sale is applied.
        live_v = verdict(pk["k"], q, paid) if pk["k"] else "none"
        alt_v = verdict(pk["alt"], q, paid) if pk["alt"] else "none"
        if pk["k"] in lst:
            v = lst[pk["k"]]
            v[ZQ] += q                     # checkout has processed this pick (sold or re-picked)
            if live_v == "ok":
                v[QTY] -= q
                st["sold"] += 1
                st["tickets"] += q
                emit(pk["k"], "zone_sold")
            else:
                emit(pk["k"], "zone_repick")   # the listing's state says checkout saw this pick
        if live_v != "ok":
            st[f"live_{live_v}"] += 1
        if alt_v != "ok":
            st[f"alt_{alt_v}"] += 1

    async def read_picks():
        # Every partition read directly, from start-up on, on its own connection.
        who = {p: Consumer.Single(f"checkout-{p}") for p in range(PARTITIONS[PICKS])}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)
        nxt = {}
        while True:
            more = False
            for p in who:
                ms = await pick_reader.poll_messages(STREAM, PICKS, consumer=who[p], partition_id=p, count=1000,
                                                     auto_commit=False,
                                                     polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_pick(m)
                    more = more or len(ms) == 1000
            if not more:
                await asyncio.sleep(0.001)

    async def report():
        while True:
            await asyncio.sleep(1)
            cutoff = time.time_ns() - 5_000_000_000
            for oid in [o for o, p in pending.items() if p[0] < cutoff]:
                del pending[oid]
                st["unpicked"] += 1        # no pick within 5 s: the fan waits on a spinner
            r = lat.flush()
            c = st["picked"]
            rep = st["live_gone"] + st["live_over"]
            alt = st["alt_gone"] + st["alt_over"]
            print(f"[listings] {st['changes']:>6,} changes/s  zone orders {st['orders']:,} picked {c:,} "
                  f"({'final hour' if run['final'] else 'steady'})  re-picks {rep:,} "
                  f"(sold out {st['live_gone']:,} over the deal price {st['live_over']:,}) no seat {st['live_none']:,}  "
                  f"a pool refreshed every {a.refresh_s:g}s would re-pick {alt:,} "
                  f"(sold out {st['alt_gone']:,} over the deal price {st['alt_over']:,}) no seat {st['alt_none']:,}  "
                  f"tickets sold {st['tickets']:,}  no deal shown {st['no_deal']:,}  unpicked {st['unpicked']:,}  "
                  f"order-to-pick p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  "
                  f"tick late max {st['late_ms']:.0f}ms  send max {st['send_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(run_market(), read_picks(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--events", type=int, default=1000, help="events on sale")
    p.add_argument("--zones", type=int, default=8, help="Zone Deals zones per venue map (up to 8)")
    p.add_argument("--listings", type=int, default=30, help="seller listings per zone")
    p.add_argument("--hot-x", type=int, default=4, help="listings multiplier for the hottest event (a bigger venue)")
    p.add_argument("--auto-share", type=float, default=0.6, help="share of listings on automatic pricing")
    p.add_argument("--rate", type=float, default=2000, help="listing changes per second, all events")
    p.add_argument("--orders", type=float, default=80, help="Zone Deals orders per second")
    p.add_argument("--final-hour", type=float, default=1, help="order multiplier on the hottest event's final hour (1 = off)")
    p.add_argument("--final-at", type=float, default=60, help="seconds after start")
    p.add_argument("--final-for", type=float, default=30)
    p.add_argument("--hot-changes", type=float, default=2000, help="extra listing changes per second on that event")
    p.add_argument("--refresh-s", type=float, default=3, help="the comparison pool's refresh, as set on zone_picker.py (label only)")
    asyncio.run(main(p.parse_args()))
