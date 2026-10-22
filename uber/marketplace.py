"""The marketplace side: rider demand and driver supply events onto the surge window lane, keyed by hexagon.

Plays --cities cities of --hexes hexagons each in one process, with the shape Uber's demand and supply feature
pipeline describes (about 45 demand events per hexagon per minute on average, counted as distinct riders and
drivers per hexagon, per product, per one-minute window):
  * demand: a rider session in a hexagon for a product (UberX, Comfort, UberXL); some hexagons are busier
    than others; a rider can open the app several times in one window and counts once;
  * supply: every driver reports every --ping-s seconds; an available driver is a supply event in the hexagon
    the driver is in, for the driver's product; drivers move to a neighbouring hexagon now and then.
Every event goes to surge-events on the partition of its hexagon, with the event time, so the surge workers
put it in the window it belongs to. A demand spike (--spike) multiplies demand in --spike-hexes hexagons of the
first city for --spike-for seconds, as when a stadium empties; supply there does not change.

The check: for every window the marketplace keeps its own distinct counts per hexagon and product, and
publishes them to window-truth once the window has closed, so each surge worker can compare.

Prints events per second.
"""
import argparse
import asyncio
import gc
import itertools
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (CITIES, DEFAULT_URL, EVENTS, PARTITIONS, PRODUCTS, STREAM, TRUTH, ensure_topics, hexagon,
                    msg, part, window)

TICK = 0.01
PRODUCT_MIX = [0.7, 0.2, 0.1]


async def main(a):
    client = IggyClient.from_connection_string(a.url)       # one connection for the events producer
    await client.connect()
    await ensure_topics(client, EVENTS, TRUTH)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await client.producer(STREAM, EVENTS, topic_partitions_count=PARTITIONS[EVENTS], mode=cfg)
    tclient = IggyClient.from_connection_string(a.url)      # and one for the truth producer
    await tclient.connect()
    truth_out = await tclient.producer(STREAM, TRUTH, topic_partitions_count=PARTITIONS[TRUTH])

    rnd = random.Random(7)
    cities = (CITIES * (a.cities // len(CITIES) + 1))[:a.cities]
    cities = [c if i < len(CITIES) else f"{c}{i}" for i, c in enumerate(cities)]
    hexes = [[hexagon(c, i) for i in range(a.hexes)] for c in cities]
    pq = {h: part(h, PARTITIONS[EVENTS]) for row in hexes for h in row}
    # Busier and quieter hexagons: a skewed weight per hexagon, the same in every run.
    cum = list(itertools.accumulate(rnd.paretovariate(1.5) for _ in range(a.hexes)))
    demand_per_s = a.cities * a.hexes * a.demand_per_min / 60
    riders = a.hexes * 25                     # rider IDs per city; a rider may open the app more than once
    drivers = [{"id": f"{c}-d{j:05d}", "ci": ci, "i": rnd.randrange(a.hexes),
                "p": rnd.choices(PRODUCTS, PRODUCT_MIX)[0]} for ci, c in enumerate(cities) for j in range(a.drivers)]
    slots = max(1, round(a.ping_s / TICK))
    due = defaultdict(list)                   # slot -> drivers reporting in that slot of every interval
    for d in drivers:
        due[rnd.randrange(slots)].append(d)
    truth = defaultdict(lambda: defaultdict(lambda: (set(), set())))   # window -> "hex|product" -> riders, drivers
    stats = defaultdict(float)
    gc.freeze()                               # keep start-up objects out of the collector's way

    async def run():
        t0 = time.monotonic()
        nxt, tick, owed, spike_owed = t0, 0, 0.0, 0.0
        async with producer:
            while True:
                t = time.monotonic() - t0
                spike = a.spike > 1 and a.spike_at <= t < a.spike_at + a.spike_for
                stats["spike"] = spike
                now_ms = time.time_ns() // 1_000_000
                w = window(now_ms, a.window_s)
                batches = defaultdict(list)

                def send(kind, h, p, uid):
                    batches[pq[h]].append(SendMessage(msg(k=kind, h=h, p=p, u=uid, t=now_ms)))
                    tw = truth[w][f"{h}|{p}"]
                    (tw[0] if kind == "d" else tw[1]).add(uid)
                    stats[kind] += 1

                owed += demand_per_s * TICK
                n, owed = int(owed), owed - int(owed)
                for _ in range(n):
                    ci = rnd.randrange(a.cities)
                    h = hexes[ci][rnd.choices(range(a.hexes), cum_weights=cum)[0]]
                    send("d", h, rnd.choices(PRODUCTS, PRODUCT_MIX)[0], f"{cities[ci]}-r{rnd.randrange(riders)}")
                if spike:                      # the stadium empties: extra riders in a few hexagons
                    spike_owed += (a.spike - 1) * a.spike_hexes * a.demand_per_min / 60 * TICK
                    n, spike_owed = int(spike_owed), spike_owed - int(spike_owed)
                    for _ in range(n):
                        h = hexes[0][rnd.randrange(a.spike_hexes)]
                        send("d", h, rnd.choices(PRODUCTS, PRODUCT_MIX)[0], f"{cities[0]}-r{rnd.randrange(riders)}")
                for d in due[tick % slots]:
                    if rnd.random() < a.move_pct / 100:
                        d["i"] = min(a.hexes - 1, max(0, d["i"] + rnd.choice((-1, 1))))
                    if rnd.random() < a.available_pct / 100:
                        send("s", hexes[d["ci"]][d["i"]], d["p"], d["id"])
                # One batch per partition, all sent at once: a hexagon's events stay in order, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                tick += 1
                nxt += TICK
                await asyncio.sleep(max(0.0, nxt - time.monotonic()))

    async def publish_truth():
        # Once a window has closed (its end, the workers' grace, and a second more), publish the distinct
        # counts per hexagon and product, in parts, so each surge worker can check its own result.
        ws_ms = int(a.window_s * 1000)
        async with truth_out:
            while True:
                await asyncio.sleep(0.2)
                now_ms = time.time_ns() // 1_000_000
                for w in sorted(x for x in truth if (x + 1) * ws_ms + (a.grace_s + 1) * 1000 < now_ms):
                    g = {k: [len(r), len(s)] for k, (r, s) in truth.pop(w).items()}
                    items = sorted(g.items())
                    chunks = [dict(items[i:i + 2000]) for i in range(0, len(items), 2000)] or [{}]
                    await truth_out.send([SendMessage(msg(w=w, i=i, n=len(chunks), g=c))
                                          for i, c in enumerate(chunks)])
                    stats["truth"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "spike" if stats["spike"] else "steady"
            print(f"[marketplace] {int(stats['d'] + stats['s']):>6,} events/s ({phase})  demand "
                  f"{int(stats['d']):,}  supply {int(stats['s']):,}  {a.cities} cities x {a.hexes:,} hexagons  "
                  f"windows reported {int(stats['truth'])}", flush=True)
            stats["d"] = stats["s"] = 0

    await asyncio.gather(run(), publish_truth(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--cities", type=int, default=4)
    p.add_argument("--hexes", type=int, default=1000, help="hexagons per city")
    p.add_argument("--demand-per-min", type=float, default=45, help="demand events per hexagon per minute")
    p.add_argument("--drivers", type=int, default=1500, help="drivers per city")
    p.add_argument("--ping-s", type=float, default=4, help="each driver reports this often")
    p.add_argument("--available-pct", type=float, default=60, help="share of driver reports that are available")
    p.add_argument("--move-pct", type=float, default=10, help="share of reports where the driver changed hexagon")
    p.add_argument("--window-s", type=float, default=60, help="the surge window (match surge_window.py)")
    p.add_argument("--grace-s", type=float, default=1, help="the surge workers' grace after a window ends")
    p.add_argument("--spike", type=float, default=1, help="demand multiplier in the spike hexagons")
    p.add_argument("--spike-hexes", type=int, default=19, help="hexagons in the spike (a cell and two rings)")
    p.add_argument("--spike-at", type=float, default=0, help="seconds after start")
    p.add_argument("--spike-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
