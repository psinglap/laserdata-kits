"""The order group tracker, fed by the fills lane from every matching engine shard.

Reads the order-group-fills topic directly, partition by partition, on --readers connections of its own (no
consumer group: a group reader visits one partition per poll, so a fill would wait for its partition's turn).
Each record is one shard's fill in one order group, with the shard's running total of contracts filled for
that group and a sequence, so repeats and replays change nothing.

For every group it keeps the contracts filled over the rolling 15-second window, summed across every shard
the group trades on, and as fills arrive:
  * TRIGGERS the group the moment the rolling total exceeds its contracts limit: every shard cancels the
    group's resting orders and rejects new ones;
  * RESETS it --reset-after-s seconds later (standing in for the market maker's Reset call), which clears the
    triggered state and the rolling count, as Kalshi documents;
and publishes each change on order-group-states, which every shard reads (engine.py).

Prints fill-to-tracker p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business numbers:
groups triggered, how many of those no single shard could have seen (no shard alone was over the limit), and
contracts filled past the trigger because a fill or the trigger was still in flight.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (DEFAULT_URL, FILLS, PARTITIONS, STATES, STREAM, WINDOW_S, Window, ensure_topics, group, limit,
                    load, msg)

WIN_NS = WINDOW_S * 1_000_000_000


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), FILLS, STATES)
    out = await (await own_client()).producer(STREAM, STATES, topic_partitions_count=PARTITIONS[STATES])
    groups = [group(i) for i in range(a.groups)]
    limits = {g: limit(g, a.fills_per_s, a.groups) for g in groups}
    lat, stats = Window(), defaultdict(float)
    seq, last = {}, {}                      # (shard, group) -> last sequence / running total applied
    win = defaultdict(deque)                # group -> (fill ns, shard, contracts) inside the rolling window
    total = defaultdict(int)                # group -> contracts in the window, all shards
    per = defaultdict(lambda: defaultdict(int))   # group -> shard -> contracts in the window
    trig = {}                               # group -> ns it was triggered (absent while active)
    since = defaultdict(int)                # group -> ns of its last reset; earlier fills don't count again
    gseq = defaultdict(int)                 # group -> last state sequence sent
    pending, resets = [], deque()           # states waiting for the next send; (due, group) resets
    caught_up = [False]                     # a new reader first replays fills from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def publish(g, state, ns):
        # The state's sequence is its time, so a restarted tracker's states still sort after older ones.
        gseq[g] = max(gseq[g] + 1, ns)
        pending.append(SendMessage(msg(g=g, seq=gseq[g], state=state, ns=ns)))

    def on_fill(m):
        f = load(m.payload())
        now = time.time_ns()
        age = (now - f["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = (f["sh"], f["g"])
        if f["seq"] <= seq.get(k, 0):
            stats["dropped"] += 1           # older or repeated fill: already counted
            return
        seq[k] = f["seq"]
        c = f["cum"] - last.get(k, 0)       # contracts in this fill, from the shard's running total
        last[k] = f["cum"]
        g, ns, sh = f["g"], f["ns"], f["sh"]
        stats["fills"] += 1
        stats["contracts"] += c
        if g in trig:                       # filled while the group was triggered somewhere in the loop
            if ns > trig[g]:
                stats["after_trigger"] += c     # the trigger was on its way to this shard
            else:
                stats["before_trigger"] += c    # this fill was on its way to the tracker
            return
        if ns < since[g]:
            stats["before_trigger"] += c        # from the episode before the last reset
            return
        q = win[g]
        q.append((ns, sh, c))
        total[g] += c
        per[g][sh] += c
        while q and q[0][0] < ns - WIN_NS:  # drop fills older than the rolling window
            ons, osh, oc = q.popleft()
            total[g] -= oc
            per[g][osh] -= oc
        if total[g] > limits[g] and caught_up[0]:   # a replayed window triggers on its next live fill
            trig[g] = now
            publish(g, "triggered", now)
            resets.append((now + int(a.reset_after_s * 1e9), g))
            stats["triggered"] += 1
            if max(per[g].values()) <= limits[g]:
                stats["cross_only"] += 1    # no single shard was over the limit: only the sum shows it

    async def resetter():
        # The market maker's Reset, after --reset-after-s: clears the triggered state and the rolling count.
        while True:
            await asyncio.sleep(0.05)
            now = time.time_ns()
            while resets and resets[0][0] <= now:
                _, g = resets.popleft()
                trig.pop(g, None)
                since[g] = now
                win[g].clear()
                total[g] = 0
                per[g].clear()
                publish(g, "active", now)
                stats["reset"] += 1

    async def send_states():
        # States are small and come in bursts; one batch every 2 ms keeps them off a send-per-state loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["states"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, FILLS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_fill(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of fills arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier fills this second", flush=True)
                for k in ("replayed", "max_age"):
                    stats[k] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            n = stats["contracts"] or 1
            past = stats["before_trigger"] + stats["after_trigger"]
            print(f"[{a.name}] {r['n']:>6,} fills/s  fill-to-tracker p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  states sent {int(stats['states']):,}", flush=True)
            print(f"[groups] triggered {int(stats['triggered']):,} (no single shard over the limit "
                  f"{int(stats['cross_only']):,})  reset {int(stats['reset']):,}  triggered now {len(trig):,}/{a.groups:,}  "
                  f"contracts {int(stats['contracts']):,}  past the trigger {int(past):,} "
                  f"(in flight to the tracker {int(stats['before_trigger']):,}, trigger in flight to a shard "
                  f"{int(stats['after_trigger']):,}; {100 * past / n:.3f}% of contracts)", flush=True)
            stats.clear()

    parts = list(range((await (await own_client()).get_topic(STREAM, FILLS)).partitions_count))
    await asyncio.gather(send_states(), resetter(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the fills partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="tracker")
    p.add_argument("--budget-ms", type=int, default=20, help="fill-to-tracker p99 budget for the lane")
    p.add_argument("--groups", type=int, default=20_000, help="must match engine.py")
    p.add_argument("--fills-per-s", type=float, default=8_000, help="must match engine.py (sets the limits)")
    p.add_argument("--reset-after-s", type=float, default=5, help="seconds from a trigger to the market maker's reset")
    asyncio.run(main(p.parse_args()))
