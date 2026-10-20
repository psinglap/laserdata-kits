"""The tracking screen and last-mile ETA, fed by the ping lane.

Keeps every partner's recent pings and current trip leg, and refreshes the ETA of every
active order every --refresh seconds, the way Swiggy's ETA posts describe: current speed
from the haversine distance between successive pings over a window, remaining distance
from the latest ping to the restaurant or the customer. Each ETA goes to the eta topic
for the tracking screen, carrying the sequence of the ping it was computed from.

Prints ping-to-tracker latency every second (from the partner's ping being created to the
tracker having applied it) with PASS or FAIL against --budget-ms. On start it rebuilds
every partner's recent pings and trip by replaying the retained topics, so it holds no state;
replayed pings are not counted as live latency.
"""
import argparse
import asyncio
import os
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, ETA, LEGS, PINGS, STREAM, Window, km, load, msg, part

ETA_PARTITIONS = 8
WAIT_GUESS_S = {"fm": 240, "wt": 120}      # restaurant wait still ahead, by leg (a stand-in for the WT model)


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    while not all([await client.get_topic(STREAM, t) for t in (PINGS, LEGS)]):
        print("[track] waiting for fleet.py to create the topics…", flush=True)
        await asyncio.sleep(1)
    started_ns = time.time_ns()
    poll = dict(batch_length=5000, auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))
    async def connected():
        # One connection per reader and writer, so a busy one never queues behind another.
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c
    pings = await (await connected()).consumer_group(f"track-pings-{os.getpid()}", STREAM, PINGS,
                                                    polling_strategy=PollingStrategy.First(), **poll)
    legs = await (await connected()).consumer_group(f"track-legs-{os.getpid()}", STREAM, LEGS,
                                                   polling_strategy=PollingStrategy.First(), **poll)
    out = await client.producer(STREAM, ETA, topic_partitions_count=ETA_PARTITIONS)
    recent = defaultdict(lambda: deque(maxlen=a.window))     # partner -> recent (seq, ns, lat, lon, order)
    trip = {}                                                # partner -> [order, leg, pickup, drop]
    lat, stats = Window(), defaultdict(int)
    to_broker, from_broker, loop_lag = Window(), Window(), Window()     # where the time goes (KIT_DEBUG=1)

    async def on_ping(m):
        p = load(m.payload())
        r = recent[p["k"]]
        if r and p["seq"] <= r[-1][0]:
            return                          # older or repeated ping: the newest position wins
        r.append((p["seq"], p["ns"], p["lat"], p["lon"], p.get("order")))
        if p["ns"] >= started_ns:           # replayed history is a rebuild, not live latency
            now = time.time_ns()
            lat.add((now - p["ns"]) / 1e6)
            to_broker.add(m.timestamp() / 1e3 - p["ns"] / 1e6)
            from_broker.add(now / 1e6 - m.timestamp() / 1e3)

    async def on_leg(m):
        e = load(m.payload())
        if e["ev"] == "assigned":
            trip[e["k"]] = [e["order"], "fm", e["pickup"], e["drop"]]
        elif e["ev"] == "delivered":
            trip.pop(e["k"], None)
        elif e["k"] in trip and trip[e["k"]][0] == e["order"]:
            trip[e["k"]][1] = "wt" if e["ev"] == "arrived" else "lm"

    def eta_s(k, t):
        r = recent.get(k)
        if not r or r[-1][4] != t[0]:
            return None, None               # no ping yet for this order: legs and pings are separate topics
        seq, ns, la, lo, _ = r[-1]
        dist = sum(km(r[i - 1][2], r[i - 1][3], r[i][2], r[i][3]) for i in range(1, len(r)))
        hours = (r[-1][1] - r[0][1]) / 3.6e12
        speed = min(40.0, max(5.0, dist / hours)) if hours > 0 and dist > 0.01 else 20.0   # current speed, km/h
        order, leg, pickup, drop = t
        if leg == "fm":
            left = km(la, lo, *pickup) + km(*pickup, *drop)
        elif leg == "wt":
            left = km(*pickup, *drop)
        else:
            left = km(la, lo, *drop)
        return round(left / speed * 3600 + WAIT_GUESS_S.get(leg, 0)), seq

    async def refresh():
        # Every active order gets a fresh ETA every --refresh seconds, spread over ten slices.
        async with out:
            while True:
                for s in range(10):
                    t_slice = time.monotonic()
                    batches = defaultdict(list)
                    for k, t in list(trip.items()):
                        if hash(k) % 10 != s:
                            continue
                        e, seq = eta_s(k, t)
                        if e is None:
                            continue
                        batches[part(k, ETA_PARTITIONS)].append(SendMessage(msg(k=k, order=t[0], leg=t[1], eta_s=e, ping_seq=seq)))
                        stats["etas"] += 1
                    await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                           for pid, ms in batches.items()))
                    await asyncio.sleep(max(0.0, a.refresh / 10 - (time.monotonic() - t_slice)))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[track] {r['n']:>7,} pings/s  ping-to-tracker p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  ETAs published {stats['etas']:,}  "
                  f"active orders {len(trip):,}", flush=True)
            stats.clear()
            if os.environ.get("KIT_DEBUG"):
                tb, fb, ll = to_broker.flush(), from_broker.flush(), loop_lag.flush()
                print(f"[debug] ping->broker p50 {tb['p50']:.1f} p99 {tb['p99']:.1f}  broker->tracker p50 {fb['p50']:.1f} "
                      f"p99 {fb['p99']:.1f}  event-loop lag p99 {ll['p99']:.1f} max {ll['max']:.1f} ms", flush=True)

    async def lag():
        while True:
            t = time.monotonic()
            await asyncio.sleep(0.01)
            loop_lag.add((time.monotonic() - t - 0.01) * 1e3)

    stop = asyncio.Event()
    await asyncio.gather(pings.consume_messages(on_ping, stop), legs.consume_messages(on_leg, stop), refresh(), report(), lag())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--refresh", type=float, default=10, help="seconds between ETA refreshes per order")
    p.add_argument("--window", type=int, default=6, help="pings per partner kept for the current-speed feature")
    p.add_argument("--budget-ms", type=int, default=200)
    asyncio.run(main(p.parse_args()))
