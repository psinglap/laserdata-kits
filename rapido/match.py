"""The matching engine's nearby-captain index and the customer's pickup tracker, fed by the captain lane.

Keeps every captain's latest ping in a grid of cells (a stand-in for the hex cells matching
works in) and, for every ride request, picks the nearest free captain within --radius-km of
the pickup and publishes an offer to ride-offers, carrying the sequence of the ping it used.
While a captain rides to a pickup, every new ping also goes out on pickup-track with the
captain's distance from the customer.

Prints ping-to-index latency every second (from the captain's ping being created to the index
having applied it) with PASS or FAIL against --budget-ms, and request-to-offer latency.
On start it rebuilds the index by replaying the retained pings, so it holds no state;
replayed pings are not counted as live latency, and requests older than --expire-s are dropped.
"""
import argparse
import asyncio
import math
import os
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, OFFERS, PICKUP, PINGS, RIDES, STREAM, Window, cell, load, msg, part, read_partitions

OFFER_PARTITIONS = PICKUP_PARTITIONS = 8


def approx_km(a_lat, a_lon, b_lat, b_lon):
    # Equirectangular distance: accurate to metres at city scale, and cheap enough for every candidate.
    dx = (b_lon - a_lon) * 111.32 * math.cos(math.radians((a_lat + b_lat) / 2))
    dy = (b_lat - a_lat) * 110.57
    return math.hypot(dx, dy)


async def main(a):
    async def connected():
        # One connection per reader and writer, so a busy one never queues behind another.
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await connected()
    while not all([await admin.get_topic(STREAM, t) for t in (PINGS, RIDES)]):
        print("[match] waiting for captains.py to create the topics…", flush=True)
        await asyncio.sleep(1)
    started_ns = time.time_ns()
    # Direct partition reads, each topic on its own connection, rebuilt from the first message.
    pings, rides = await connected(), await connected()
    ping_parts = range((await admin.get_topic(STREAM, PINGS)).partitions_count)
    ride_parts = range((await admin.get_topic(STREAM, RIDES)).partitions_count)
    offers_p = await (await connected()).producer(STREAM, OFFERS, topic_partitions_count=OFFER_PARTITIONS)
    track_p = await (await connected()).producer(STREAM, PICKUP, topic_partitions_count=PICKUP_PARTITIONS)

    pos = {}                                  # captain -> [seq, ns, lat, lon, free, cell]
    cells = defaultdict(set)                  # cell -> free, unreserved captains last seen there
    reserved = {}                             # captain -> monotonic deadline: offered, not yet seen busy
    to_pickup = {}                            # captain -> (ride, pickup lat, pickup lon)
    out_offers, out_track = [], []
    lat, req_lat, stats = Window(), Window(), defaultdict(int)
    live = False                              # becomes True after a whole second of pings inside the budget

    def unindex(k):
        p = pos.get(k)
        if p and p[5] is not None:
            cells[p[5]].discard(k)
            p[5] = None

    async def on_ping(m):
        p = load(m.payload())
        k, cur = p["k"], pos.get(p["k"])
        if cur and p["seq"] <= cur[0]:
            return                            # older or repeated ping: the newest position wins
        free = p["st"] == "free"
        unindex(k)
        pos[k] = cur = [p["seq"], p["ns"], p["lat"], p["lon"], free, None]
        if free:
            to_pickup.pop(k, None)
            if k not in reserved or reserved[k] < time.monotonic():
                reserved.pop(k, None)
                cur[5] = cell(p["lat"], p["lon"])
                cells[cur[5]].add(k)
        else:
            reserved.pop(k, None)
            t = to_pickup.get(k)
            if t and p.get("ride") == t[0]:
                d = approx_km(p["lat"], p["lon"], t[1], t[2])
                out_track.append((k, SendMessage(msg(ride=t[0], k=k, ping_seq=p["seq"], d_km=round(d, 3)))))
                stats["track"] += 1
                if d < 0.05:
                    to_pickup.pop(k, None)    # arrived: pickup tracking ends
        age = (time.time_ns() - p["ns"]) / 1e6
        if not live:                          # still replaying: track the oldest ping seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
            if p["ns"] >= started_ns and age >= a.budget_ms:
                stats["catching_up"] += 1
        elif p["ns"] >= started_ns:           # replayed history is a rebuild, not live latency
            lat.add(age)

    async def on_ride(m):
        r = load(m.payload())
        now_ns = time.time_ns()
        if (now_ns - r["ns"]) / 1e9 > a.expire_s:
            stats["expired"] += 1             # a request from before this run: the customer has moved on
            return
        plat, plon = r["pickup"]
        cx, cy = cell(plat, plon)
        best, best_d = None, a.radius_km
        for ring in range(0, math.ceil(a.radius_km / 1.05) + 1):     # grow ring by ring around the pickup cell
            if best is not None and best_d <= (ring - 1) * 1.05:
                break                                                 # nothing further out can be nearer
            for dx in range(-ring, ring + 1):
                for dy in range(-ring, ring + 1):
                    if max(abs(dx), abs(dy)) != ring:
                        continue
                    for k in cells.get((cx + dx, cy + dy), ()):
                        q = pos[k]
                        d = approx_km(plat, plon, q[2], q[3])
                        if d < best_d:
                            best, best_d = k, d
        stats["requests"] += 1
        if best is None:
            stats["unmatched"] += 1
            return
        q = pos[best]
        unindex(best)
        reserved[best] = time.monotonic() + a.hold_s
        to_pickup[best] = (r["ride"], plat, plon)
        out_offers.append((best, SendMessage(msg(ride=r["ride"], k=best, ping_seq=q[0], used=[q[2], q[3]],
                                                 d_km=round(best_d, 3), pickup=r["pickup"], drop=r["drop"],
                                                 req_ns=r["ns"]))))
        req_lat.add((time.time_ns() - r["ns"]) / 1e6)
        stats["offers"] += 1

    async def flush(producer, buf, partitions):
        # Every few ms, one batch per partition, sent at once.
        async with producer:
            while True:
                if buf:
                    items = buf[:]
                    buf.clear()
                    batches = defaultdict(list)
                    for key, sm in items:
                        batches[part(key, partitions)].append(sm)
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                           for pid, ms in batches.items()))
                await asyncio.sleep(0.002)

    async def report():
        nonlocal live
        while True:
            await asyncio.sleep(1)
            r, q = lat.flush(), req_lat.flush()
            if not live:
                # live once a whole second of pings arrived inside the budget; partitions interleave,
                # so one fresh ping alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    live = True
                print(f"[match] catching up: {stats['catching_up']:,} live pings older than {a.budget_ms} ms "
                      f"(replaying the backlog), index {len(pos):,} captains", flush=True)
            else:
                ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
                free = sum(len(s) for s in cells.values())
                print(f"[match] {r['n']:>7,} pings/s  ping-to-index p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                      f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  requests {stats['requests']:,}/s  "
                      f"offers {stats['offers']:,}  no captain in {a.radius_km:g} km {stats['unmatched']:,}  "
                      f"request-to-offer p99 {q['p99']:5.1f}ms  free in index {free:,}  pickup-track {stats['track']:,}",
                      flush=True)
            stats.clear()

    await asyncio.gather(read_partitions(pings, PINGS, ping_parts, f"match-pings-{os.getpid()}", on_ping, replay=True,
                                         poll_ms=0.5),
                         read_partitions(rides, RIDES, ride_parts, f"match-rides-{os.getpid()}", on_ride, replay=True,
                                         poll_ms=1),
                         flush(offers_p, out_offers, OFFER_PARTITIONS), flush(track_p, out_track, PICKUP_PARTITIONS),
                         report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--radius-km", type=float, default=3.0, help="search radius around the pickup")
    p.add_argument("--hold-s", type=float, default=20, help="seconds an offered captain stays out of the index")
    p.add_argument("--expire-s", type=float, default=30, help="ride requests older than this are dropped")
    p.add_argument("--budget-ms", type=int, default=200)
    asyncio.run(main(p.parse_args()))
