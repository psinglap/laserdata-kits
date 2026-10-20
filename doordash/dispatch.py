"""The dispatch coordinator, fed by the store feedback lane.

Keeps the latest feedback for every store from the store-feedback topic: Dashers heading there,
Dashers waiting at the counter, the last five Dasher waits and the market's utilization. New
orders arrive at --orders-per-s. Every --run-ms the coordinator runs dispatch over the orders
that arrived since the last run; for each one, policy() (a stand-in for the RL agent) reads that
store's feedback as it is at the start of the run and leans toward delivery speed or batching.
Each decision goes to the dispatch-decisions topic carrying the run time and the feedback
sequence it was made on, so dashers.py can check it against the truth.

Prints event-to-coordinator latency every second (from the Dasher event being created to this
process having applied it) with PASS or FAIL against --budget-ms. On start it rebuilds every
store's feedback by replaying the topic, so it holds no state; replayed events are not counted,
and it prints "catching up" until a whole second of live feedback lands inside the budget.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DECISION_PARTITIONS, DECISIONS, DEFAULT_URL, FEEDBACK, STREAM, GcPauses, Popularity, Window, load,
                    msg, part, policy)


async def connected(url):
    # One connection per reader and writer, so a busy one never queues behind another.
    c = IggyClient.from_connection_string(url)
    await c.connect()
    return c


async def main(a):
    client = await connected(a.url)
    while not all([await client.get_topic(STREAM, t) for t in (FEEDBACK, DECISIONS)]):
        print("[dispatch] waiting for dashers.py to create the topics…", flush=True)
        await asyncio.sleep(1)
    started_ns = time.time_ns()
    lane = await (await connected(a.url)).consumer_group(
        f"dispatch-coordinator-{os.getpid()}", STREAM, FEEDBACK, polling_strategy=PollingStrategy.First(), batch_length=5000,
        auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))
    out = await (await connected(a.url)).producer(STREAM, DECISIONS, topic_partitions_count=DECISION_PARTITIONS,
                                                  mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200)))
    pop = Popularity(a.stores, a.skew)
    gc.freeze()            # startup tables live all run; keep them out of later GC passes
    view, lat, stats = {}, Window(), defaultdict(int)
    to_broker, from_broker, gc_pause = Window(), Window(), GcPauses()     # where the time goes (--why)

    async def on_feedback(m):
        e = load(m.payload())
        v = view.get(e["r"])
        if v is not None and e["seq"] <= v["seq"]:
            return                          # older or repeated change: the newest feedback wins
        view[e["r"]] = e
        if e["seq"] > 1 and e["ns"] >= started_ns:      # seq 1 is the snapshot; replayed history is a rebuild
            now = time.time_ns()
            lat.add((now - e["ns"]) / 1e6)
            if a.why:
                to_broker.add(m.timestamp() / 1e3 - e["ns"] / 1e6)
                from_broker.add(now / 1e6 - m.timestamp() / 1e3)

    async def runs():
        n, owed, t0 = 0, 0.0, time.monotonic()
        due = t0
        async with out:
            while True:
                t = time.monotonic() - t0
                rush = a.rush if a.rush_at and a.rush_at <= t < a.rush_at + a.rush_for else 1
                owed += a.orders_per_s * rush * a.run_ms / 1000
                run_ns = time.time_ns()                   # every decision in this run reads the view as it is now
                batches = defaultdict(list)
                while owed >= 1:
                    owed -= 1
                    n += 1
                    r = pop.pick()
                    fb = view.get(r)
                    mode, w = policy(fb)
                    batches[part(r, DECISION_PARTITIONS)].append(SendMessage(msg(
                        order=f"D{os.getpid()}-{n}", r=r, mode=mode, w=w, seen=fb["seq"] if fb else 0, run_ns=run_ns)))
                    stats["decisions"] += 1
                    stats["batch"] += mode == "batch"
                await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for pid, ms in batches.items()))
                stats["runs"] += 1
                due += a.run_ms / 1000
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        frozen, caught_up = False, False
        while True:
            await asyncio.sleep(1)
            if not frozen and len(view) >= a.stores:
                gc.freeze()        # every store is in the view: freeze it once, as dashers.py does after its snapshot
                frozen = True
            r = lat.flush()
            d = stats["decisions"]
            share = 100 * stats["batch"] / d if d else 0
            if not caught_up:
                # A new coordinator first replays earlier feedback; it is live only after a whole second inside the budget.
                caught_up = r["n"] > 0 and r["max"] < a.budget_ms
                if not caught_up:
                    print(f"[dispatch] catching up: {len(view):,} stores in view, {r['n']:,} live events, "
                          f"slowest {r['max']:.0f} ms", flush=True)
                    stats.clear()
                    continue
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[dispatch] {r['n']:>6,} feedback events/s  event-to-coordinator p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  dispatch runs {stats['runs']}  decisions {d:,}  "
                  f"leaning to batching {share:4.1f}%  stores in view {len(view):,}", flush=True)
            stats.clear()
            if a.why:
                tb, fb = to_broker.flush(), from_broker.flush()
                print(f"[why] dasher event->broker p50 {tb['p50']:.1f} p99 {tb['p99']:.1f} max {tb['max']:.1f}  "
                      f"broker->coordinator p50 {fb['p50']:.1f} p99 {fb['p99']:.1f} max {fb['max']:.1f} ms  "
                      f"longest GC pause {gc_pause.take():.0f} ms", flush=True)

    await asyncio.gather(lane.consume_messages(on_feedback, asyncio.Event()), runs(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--stores", type=int, default=200_000, help="must match dashers.py")
    p.add_argument("--skew", type=float, default=0.6, help="must match dashers.py")
    p.add_argument("--orders-per-s", type=float, default=125, help="new orders a second (Q2 2026 average: 970M orders a quarter)")
    p.add_argument("--run-ms", type=float, default=1000, help="time between dispatch runs (illustrative)")
    p.add_argument("--rush", type=float, default=1.0, help="multiplier for orders during the rush")
    p.add_argument("--rush-at", type=float, default=0)
    p.add_argument("--rush-for", type=float, default=120)
    p.add_argument("--budget-ms", type=int, default=100)
    p.add_argument("--why", action="store_true", help="also print where the time goes: Dasher event to broker, broker to coordinator")
    asyncio.run(main(p.parse_args()))
