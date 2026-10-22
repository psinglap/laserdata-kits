"""witness_assembler on the lane: every upload batch, per service, in milliseconds, and every batch accounted for.

Reads the witness-batches topic directly, partition by partition, on --readers connections of its own (no
consumer group: a group reader visits one partition per poll, so with 16 partitions a batch waits for its
partition's turn). A service's batches share one partition, so each agent's batches arrive in order.

For each batch it checks the agent's batch number against the last one applied for that client_id:
  * the next number: applied, its witnesses added to the service's endpoints;
  * a number already applied (a retried publish, or a replay after a restart): counted once, adds nothing;
  * a jump: the batches in between never arrived, and they are named (client_id and batch numbers), which
    is the "batch that never arrived" neither side could see before.
Apache Iggy delivers at least once; counting once comes from this rule, which is why every batch carries
the agent's number.

Prints, every second: batch publish-to-assembler p50/p99/p99.9 with PASS/FAIL against --budget-ms, and the
funnel: batches and witnesses assembled, missing batches named, duplicates absorbed, and what the
Insights overview would show for the second (requests observed, server errors, p90 API latency).
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, STREAM, WITNESSES, Window, ensure_topics, load


async def main(a):
    async def own_client():                 # one connection per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), WITNESSES)
    lat, api, stats = Window(), Window(), defaultdict(float)
    last = {}                               # client_id -> last batch number applied
    tot = defaultdict(int)                  # since start
    eps = set()                             # (service, endpoint) seen
    services = set()
    named = []                              # missing batches named this second, for the log
    caught_up = [False]                     # a new reader first replays batches from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_batch(m):
        d = load(m.payload())
        now = time.time_ns()
        age = (now - d["ns"]) / 1e6
        c, b = d["c"], d["b"]
        prev = last.get(c, 0)
        if b <= prev:
            tot["duplicates"] += 1          # a retried publish or a replay: already counted
            return
        if b > prev + 1:                    # the agent's numbers jumped: those uploads never arrived
            tot["missing"] += b - prev - 1
            named.append(f"{c} batch {prev + 1}" + (f"-{b - 1}" if b - 1 > prev + 1 else ""))
        last[c] = b
        if not caught_up[0]:                # still replaying: track the oldest new batch seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        else:
            lat.add(age)                    # live: from the publish to witness_assembler having the batch
        s = d["s"]
        services.add(s)
        w = d["w"]
        tot["batches"] += 1
        tot["witnesses"] += len(w)
        stats["batches"] += 1
        stats["witnesses"] += len(w)
        for ep, status, ms in w:            # what the Insights overview counts: requests, errors, latency
            eps.add((s, ep))
            if status >= 500:
                stats["errors"] += 1
        for i in range(0, len(w), 10):      # one latency in ten is plenty for a p90
            api.add(w[i][2])
        if d.get("last"):
            tot["closed"] += 1              # the agent's last batch on shutdown

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, WITNESSES, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_batch(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of batches arrived inside the budget; partitions interleave,
                # so one fresh batch alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier batches this second",
                      flush=True)
                stats.clear()
                lat.flush()
                api.flush()
                named.clear()
                continue
            r, q = lat.flush(), api.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {int(stats['batches']):>5,} batches/s {int(stats['witnesses']):>7,} witnesses/s  "
                  f"publish-to-assembler p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  "
                  f"{ok} (<{a.budget_ms}ms)", flush=True)
            print(f"[funnel] assembled {tot['batches']:,} batches, {tot['witnesses']:,} witnesses from "
                  f"{len(last):,} agents  missing batches {tot['missing']:,}  duplicates absorbed "
                  f"{tot['duplicates']:,}  agents closed {tot['closed']:,}  |  {len(services):,} services, "
                  f"{len(eps):,} endpoints  this second: {int(stats['witnesses']):,} requests, "
                  f"{int(stats['errors']):,} 5xx, API p90 {q['p90']:.0f} ms", flush=True)
            if named:
                print(f"[funnel] never arrived: {', '.join(named[:4])}" +
                      (f" and {len(named) - 4} more" if len(named) > 4 else ""), flush=True)
                named.clear()
            stats.clear()

    parts = list(range((await (await own_client()).get_topic(STREAM, WITNESSES)).partitions_count))
    await asyncio.gather(report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the lane's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--name", default="witness_assembler")
    p.add_argument("--budget-ms", type=int, default=50, help="publish-to-assembler p99 budget for the lane")
    asyncio.run(main(p.parse_args()))
