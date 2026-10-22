"""The live trip service, fed by the driver ping lane: the tracking link and the arrival checks.

Keeps every driver's latest ping and every open order (from trip-events). For each order it
holds what the tracking link shared with the sender and receiver shows: status, the driver's
latest position and how long ago it was taken. On every ping it runs the fence check the
geospatial pipeline does: is the driver within 150 m of the pickup, or of the drop once the goods
are on board? The first ping inside a fence publishes reached_pickup or reached_drop to
trip-milestones, carrying the sequence of that ping, for the notifications and webhooks.

Prints ping-to-tracker latency every second (from the driver's ping being created to this
service having applied it) with PASS or FAIL against --budget-ms. On start it rebuilds every
driver and order by replaying the retained topics, so it holds no state; replayed pings are not
counted as live latency, and arrivals older than --resend-s are not sent again.
"""
import argparse
import asyncio
import os
import time
from collections import defaultdict
from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, FENCE_KM, MILESTONES, PINGS, STREAM, TRIPS, Window, km, load, msg, part, read_partitions

MS_PARTITIONS = 8


async def main(a):
    async def connected():
        # One connection per reader and writer, so a busy one never queues behind another.
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await connected()
    while not all([await admin.get_topic(STREAM, t) for t in (PINGS, TRIPS)]):
        print("[trips] waiting for drivers.py to create the topics…", flush=True)
        await asyncio.sleep(1)
    started_ns = time.time_ns()
    ping_parts = range((await admin.get_topic(STREAM, PINGS)).partitions_count)
    trip_parts = range((await admin.get_topic(STREAM, TRIPS)).partitions_count)
    pings, trips = await connected(), await connected()     # each reader on its own connection
    out = await (await connected()).producer(STREAM, MILESTONES, topic_partitions_count=MS_PARTITIONS)

    last = {}                                  # driver -> (seq, ns, lat, lon, order)
    orders = {}                                # order -> [driver, status, pickup, drop, reached set]
    buf = []
    lat, stats = Window(), defaultdict(int)
    to_broker, from_broker = Window(), Window()     # where the time goes (--why)
    live = False                               # becomes True after a whole second of pings inside the budget

    async def on_ping(m):
        p = load(m.payload())
        k, cur = p["k"], last.get(p["k"])
        if cur and p["seq"] <= cur[0]:
            return                             # older or repeated ping: the newest position wins
        last[k] = (p["seq"], p["ns"], p["lat"], p["lon"], p.get("order"))
        o = orders.get(p.get("order"))
        if o is not None and o[0] == k:
            # the fence check: the pickup until the goods are on board, then the drop
            ms, tgt = ("reached_pickup", o[2]) if o[1] == "accepted" else ("reached_drop", o[3])
            if ms not in o[4] and km(p["lat"], p["lon"], *tgt) <= FENCE_KM:
                o[4].add(ms)
                if p["ns"] >= started_ns - a.resend_s * 1e9:     # a rebuild does not re-send old arrivals
                    buf.append((k, SendMessage(msg(k=k, order=p["order"], ms=ms, ping_seq=p["seq"], ping_ns=p["ns"]))))
                    stats[ms] += 1
        now = time.time_ns()
        age = (now - p["ns"]) / 1e6
        if not live:                           # still replaying: track the oldest ping seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
            if p["ns"] >= started_ns and age >= a.budget_ms:
                stats["catching_up"] += 1
        elif p["ns"] >= started_ns:            # replayed history is a rebuild, not live latency
            lat.add(age)
            if a.why:
                to_broker.add(m.timestamp() / 1e3 - p["ns"] / 1e6)
                from_broker.add(now / 1e6 - m.timestamp() / 1e3)

    async def on_trip(m):
        e = load(m.payload())
        if e["ev"] == "accepted":
            orders[e["order"]] = [e["k"], "accepted", tuple(e["pickup"]), tuple(e["drop"]), set()]
        elif e["ev"] == "live" and e["order"] in orders:
            orders[e["order"]][1] = "live"
        elif e["ev"] == "ended":
            orders.pop(e["order"], None)

    async def flush():
        # Every few ms, one batch per partition, sent at once.
        async with out:
            while True:
                if buf:
                    items = buf[:]
                    buf.clear()
                    batches = defaultdict(list)
                    for key, sm in items:
                        batches[part(key, MS_PARTITIONS)].append(sm)
                    await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                           for pid, ms in batches.items()))
                await asyncio.sleep(0.002)

    def link_age_p99():
        # What the tracking links show right now: age of the driver position on each open order.
        now = time.time_ns()
        ages = sorted((now - last[o[0]][1]) / 1e9 for o in orders.values() if o[0] in last)
        return ages[int(0.99 * (len(ages) - 1))] if ages else 0.0

    async def report():
        nonlocal live
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            if not live:
                # live once a whole second of pings arrived inside the budget; partitions interleave,
                # so one fresh ping alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    live = True
                print(f"[trips] catching up: {stats['catching_up']:,} live pings older than {a.budget_ms} ms "
                      f"(replaying the backlog), drivers {len(last):,}, open orders {len(orders):,}", flush=True)
            else:
                ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
                print(f"[trips] {r['n']:>7,} pings/s  ping-to-tracker p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                      f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  open orders {len(orders):,}  "
                      f"tracking-link position age p99 {link_age_p99():4.1f}s  "
                      f"arrivals reached_pickup {stats['reached_pickup']:,} reached_drop {stats['reached_drop']:,}",
                      flush=True)
                if a.why:
                    tb, fb = to_broker.flush(), from_broker.flush()
                    print(f"[why]   ping->broker p50 {tb['p50']:.1f} p99 {tb['p99']:.1f}  "
                          f"broker->tracker p50 {fb['p50']:.1f} p99 {fb['p99']:.1f} ms", flush=True)
            stats.clear()

    # Both readers rebuild from the first retained message on every start, so no offset is stored.
    await asyncio.gather(read_partitions(pings, PINGS, ping_parts, f"trips-pings-{os.getpid()}", on_ping, replay=True, poll_ms=0.5),
                         read_partitions(trips, TRIPS, trip_parts, f"trips-events-{os.getpid()}", on_trip, replay=True, poll_ms=2),
                         flush(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--budget-ms", type=int, default=200)
    p.add_argument("--resend-s", type=float, default=30, help="on a rebuild, arrivals older than this are not sent again")
    p.add_argument("--why", action="store_true", help="also print where the time goes: ping to broker, broker to tracker")
    asyncio.run(main(p.parse_args()))
