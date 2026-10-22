"""The device throttle service, fed by the device-counts lane.

Reads the device-counts topic directly, partition by partition, on --readers connections of its own (no consumer
group: a group reader visits one partition per poll, so with 16 partitions a record waits for its partition's
turn). Each record is one ingestion node's running count of accepted events for one device in one second (the
resulting count, with a sequence), so a device's rate is the sum over every node, and replays or repeats change
nothing.

For every device it keeps one-second buckets and the average over the last --window-s seconds, the way
Amplitude's docs describe the throttle ("measured as an average over a recent time window"), and decides:
  * THROTTLED the moment the average reaches --threshold events per second, across all nodes together;
  * OK once the average falls back under it (checked every second, also for devices that went quiet);
and publishes each change on throttle-state, which every ingestion node reads (ingest_nodes.py). On start-up it
first replays throttle-state, so it continues where the last run stopped and lifts throttles that ran out.

Prints count-to-throttle p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business number:
events let in past the throttle, the events accepted from a device after its average had crossed the
threshold, because the count or the throttle was still in flight.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (COUNTS, DEFAULT_URL, STATE, STREAM, THRESHOLD_EPS, WINDOW_S, Rate, Window, ensure_topics,
                    load, msg)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), COUNTS, STATE)
    state = {}                              # device -> [decision seq, "throttled" | "ok", crossing ns]
    # Rebuild the decisions already published, so sequences continue and stale throttles are lifted below.
    c0, nxt = await own_client(), None
    who0 = Consumer.Single(f"{a.name}-state-{os.getpid()}")
    while True:
        ms = await c0.poll_messages(STREAM, STATE, consumer=who0, partition_id=0, count=5000, auto_commit=False,
                                    polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                    else PollingStrategy.First())
        for m in ms:
            s = load(m.payload())
            state[s["d"]] = [s["q"], s["st"], 0]
        if ms:
            nxt = ms[-1].offset() + 1
        if len(ms) < 5000:
            break
    print(f"[{a.name}] rebuilt {len(state):,} device decisions from throttle-state", flush=True)

    out = await (await own_client()).producer(STREAM, STATE, topic_partitions_count=1)
    rate = defaultdict(Rate)                # device -> one-second buckets, all nodes together
    lastrec = {}                            # (node, device) -> (seq, second, running count)
    lat, stats = Window(), defaultdict(float)
    pending = []                            # decisions waiting for the next send (one batch per 2 ms)
    run = defaultdict(float)                # totals for the run
    caught_up = [False]                     # a new reader first replays counts from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def decide(d, new, eps, since=0):
        st = state.get(d)
        q = (st[0] if st else 0) + 1
        state[d] = [q, new, since]
        pending.append(SendMessage(msg(d=d, st=new, eps=round(eps, 1), q=q)))
        if new == "throttled":
            run["throttles"] += 1
        else:
            run["released"] += 1

    def on_count(m):
        r = load(m.payload())
        now = time.time_ns()
        age = (now - r["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = (r["n"], r["d"])
        prev = lastrec.get(k)
        if prev and r["q"] <= prev[0]:
            stats["dropped"] += 1           # older or repeated count: the newest wins
            return
        delta = r["c"] - (prev[2] if prev and prev[1] == r["s"] else 0)
        lastrec[k] = (r["q"], r["s"], r["c"])
        stats["records"] += 1
        sec = int(now // 1_000_000_000)
        if r["s"] <= sec - a.window_s or delta <= 0:
            return                          # outside the window: it no longer moves the average
        d = r["d"]
        st = state.get(d)
        if st and st[1] == "throttled":
            if st[2] and r["ns"] > st[2]:
                run["past"] += delta        # accepted after this device's average had crossed the threshold
            rate[d].add(r["s"], delta)
            return
        rate[d].add(r["s"], delta)
        avg = rate[d].avg(sec, a.window_s)
        if avg >= a.threshold:
            b = rate[d].b
            run["late1s"] += max(b.get(r["s"], 0), b.get(r["s"] - 1, 0))   # about one second of its traffic
            decide(d, "throttled", avg, since=r["ns"])

    async def send_decisions():
        # Decisions are small and come in bursts; one batch every 2 ms keeps them off a send-per-decision loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["decisions"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (rates are rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, COUNTS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_count(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            sec = int(time.time())
            # Lift throttles whose average fell back under the threshold, including devices that went quiet
            # while their SDK paused after the 429.
            for d in [d for d, st in state.items() if st[1] == "throttled"]:
                avg = rate[d].avg(sec, a.window_s) if d in rate else 0.0
                if avg < a.threshold:
                    decide(d, "ok", avg)
            if not caught_up[0]:
                # caught up once a whole second of counts arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier count records this second",
                      flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} count records/s  count-to-throttle p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"decisions sent {int(stats['decisions']):,}", flush=True)
            now_thr = sum(1 for st in state.values() if st[1] == "throttled")
            n = run["throttles"]
            print(f"[spam] throttled now {now_thr:,}  throttles {int(n):,}  released {int(run['released']):,}  "
                  f"events let in past the throttle {int(run['past']):,} ({run['past'] / n if n else 0:.1f} per throttle)  "
                  f"a throttle 1 s later: about {int(run['late1s']):,} more", flush=True)
            stats.clear()
            if sec % 10 == 0:               # forget devices that have gone quiet
                for d in [d for d, rt in rate.items() if not rt.b or max(rt.b) < sec - a.window_s]:
                    if d not in state or state[d][1] != "throttled":
                        del rate[d]

    parts = list(range((await (await own_client()).get_topic(STREAM, COUNTS)).partitions_count))
    await asyncio.gather(send_decisions(), report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the device-counts partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="throttle")
    p.add_argument("--budget-ms", type=int, default=50, help="count-to-throttle p99 budget for the lane")
    p.add_argument("--threshold", type=float, default=THRESHOLD_EPS, help="events per second per device (HTTP V2: 30)")
    p.add_argument("--window-s", type=int, default=WINDOW_S, help="seconds the rate is averaged over")
    asyncio.run(main(p.parse_args()))
