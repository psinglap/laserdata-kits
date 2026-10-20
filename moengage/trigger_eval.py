"""The Smart Trigger evaluator, reading every workspace at its own rate.

One shard of the evaluator: it owns the workspaces whose number % --shards == --shard and
reads each of their partitions directly (its own offsets, no consumer group), in turn, on
--readers connections. Every workspace partition has a token bucket: a workspace may be
evaluated at up to --quota events a second. A brand that bursts past its quota queues on its
own topic, and every other brand on the shard is read as if nothing happened.

For each event it drops repeats (per-user sequence; delivery is at least once), times it,
and when the event matches a live campaign ("Added To Cart" -> push, "OTP Requested" -> SMS,
"Purchase Completed" -> WhatsApp, all sent Immediately) writes a fire to trigger-fires for
the channel senders. Prints a line a second:
  * event-to-evaluator latency for workspaces inside their quota, PASS or FAIL against --budget-ms;
  * how many of those workspaces had an event over budget this second;
  * campaign triggers fired, and the share fired within 1 s of the event;
  * any workspace held at its quota: rate read, backlog on its own topic, oldest event waiting.
"""
import argparse
import asyncio
import gc
import time
import zlib
from collections import defaultdict

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

BANK = 0.5                                                # seconds of quota a partition may bank for a burst

from common import (CAMPAIGNS, DEFAULT_URL, FIRES, FIRES_PARTITIONS, STREAM, Window, connect, ensure_topic, load, msg,
                    partitions_for, ws_name)


async def main(a):
    name = f"eval-{a.shard}"
    admin = await connect(a.url)
    if a.workspaces:                                      # the kit's own workspaces, ws-0001 … from workspaces.py
        n = a.workspaces
        mine = {ws_name(i): partitions_for(i, n) for i in range(n) if i % a.shards == a.shard}
        while not all([await admin.get_topic(STREAM, t) for t in mine]):
            print(f"[{name}] waiting for workspaces.py to create the workspace topics…", flush=True)
            await asyncio.sleep(1)
    else:                                                 # whatever ws-* topics exist, e.g. from kafka_bridge.py
        mine = {t.name: t.partitions_count for t in await admin.get_topics(STREAM)
                if t.name.startswith("ws-") and zlib.crc32(t.name.encode()) % a.shards == a.shard}
    await ensure_topic(admin, STREAM, FIRES, FIRES_PARTITIONS)
    plist = [(t, pid) for t, parts in mine.items() for pid in range(parts)]
    per_part = {(t, pid): a.quota / mine[t] for t, pid in plist}     # events/s per partition
    tokens = {k: per_part[k] * BANK for k in plist}
    refilled = {k: time.monotonic() for k in plist}
    nxt, live, held_at = {}, set(), {}                    # next offset; partitions past their replay; when last held
    last_seq = {}
    budget_ns = a.budget_ms * 1_000_000
    lat, held_wait = Window(), defaultdict(float)
    st = defaultdict(int)
    late_ws, read_ws = set(), set()
    fires = []
    clear = {}                                            # workspace -> when it was first held, for the drain report
    gc.freeze()                                           # keep startup tables out of the collector's way

    def apply(i, pid, ms, now):
        held = now - held_at.get(i, 0) < 2e9              # this workspace hit its quota in the last 2 s
        for m in ms:
            e = load(m.payload())
            k = (i, e["u"])
            if e["seq"] <= last_seq.get(k, 0):
                st["repeats"] += 1                        # a repeat or a replayed event: evaluate once
                continue
            last_seq[k] = e["seq"]
            age = now - e["ns"]
            if (i, pid) not in live:
                if age >= budget_ns:
                    st["replayed"] += 1                   # written before this evaluator started: not live latency
                    continue
                live.add((i, pid))
            if held:
                st["held_" + str(i)] += 1
                held_wait[i] = max(held_wait[i], age / 1e9)
            else:
                lat.add(age / 1e6)
                read_ws.add(i)
                st["live"] += 1
                if age > budget_ns:
                    late_ws.add(i)
            ch = CAMPAIGNS.get(e["e"])
            if ch:                                        # the trigger matched a live campaign: fire it now
                st["fired"] += 1
                if not held and age <= 1e9:
                    st["fired_1s"] += 1
                if not held:
                    st["fired_live"] += 1
                fires.append(SendMessage(msg(ws=i, u=e["u"], ch=ch, ev=e["e"], ev_ns=e["ns"])))

    async def reader(parts):
        c = await connect(a.url)
        consumers = {k: Consumer.Single(f"{name}-{k[0]}-{k[1]}") for k in parts}
        while True:
            more = False
            for k in parts:
                now = time.monotonic()
                tokens[k] = min(per_part[k] * BANK, tokens[k] + (now - refilled[k]) * per_part[k])
                refilled[k] = now
                allow = min(a.batch, int(tokens[k]))
                if allow < 1:
                    continue
                start = PollingStrategy.Offset(nxt[k]) if k in nxt else PollingStrategy.First()
                ms = await c.poll_messages(STREAM, k[0], consumer=consumers[k], partition_id=k[1],
                                           count=allow, auto_commit=False, polling_strategy=start)
                if ms:
                    tokens[k] -= len(ms)
                    nxt[k] = ms[-1].offset() + 1
                    if len(ms) == allow < a.batch:        # the quota, not the topic, ended this read:
                        held_at[k[0]] = time.time_ns()    # the rest waits on the workspace's own topic
                        clear.setdefault(k[0], time.monotonic())
                    apply(k[0], k[1], ms, time.time_ns())
                    more = more or len(ms) == a.batch     # a full batch inside quota: read on at once
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def flush_fires(producer):
        while True:
            await asyncio.sleep(0.01)
            if fires:
                batch = fires[:]
                fires.clear()
                await producer.send_messages(STREAM, FIRES, Partitioning.balanced(), batch)

    async def backlog(i):
        t = await admin.get_topic(STREAM, i)
        # every message in the topic minus every one this shard has read (it reads from the first offset)
        return max(0, t.messages_count - sum(nxt.get((i, pid), 0) for pid in range(mine[i]))) if t else 0

    async def report():
        while True:
            await asyncio.sleep(1)
            if not live:
                print(f"[{name}] waiting for live events (replayed {st['replayed']:,} earlier ones)", flush=True)
                st.clear()
                lat.flush()
                continue
            r = lat.flush()
            quiet = len(mine) - sum(1 for i in mine if time.time_ns() - held_at.get(i, 0) < 2e9)
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            within = 100 * st["fired_1s"] / st["fired_live"] if st["fired_live"] else 100.0
            print(f"[{name}] {r['n']:>6,} trigger events/s from {len(read_ws)} workspaces  event-to-evaluator "
                  f"p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"over budget {len(late_ws)}/{quiet} workspaces  fired {st['fired']:,}/s  "
                  f"within 1 s {within:6.2f}%", flush=True)
            for i in sorted(clear):
                b = await backlog(i)
                if b > a.batch or (time.time_ns() - held_at.get(i, 0) < 2e9 and st["held_" + str(i)]):
                    print(f"[{name}] held at quota: {i} read {st['held_' + str(i)]:,}/s (quota "
                          f"{a.quota:,.0f}/s)  backlog {b:,} on its own topic  oldest waiting {held_wait[i]:5.1f} s",
                          flush=True)
                else:
                    print(f"[{name}] {i} back inside its quota: backlog cleared "
                          f"{time.monotonic() - clear[i]:.1f} s after it was first held", flush=True)
                    del clear[i]
                held_wait[i] = 0.0
            st.clear()
            late_ws.clear()
            read_ws.clear()

    producer = await connect(a.url)
    groups = [plist[r::a.readers] for r in range(a.readers)]
    print(f"[{name}] reading {len(mine)} workspaces, {len(plist)} partitions, on {a.readers} connections", flush=True)
    await asyncio.gather(report(), flush_fires(producer), *(reader(g) for g in groups if g))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--shard", type=int, default=0)
    p.add_argument("--shards", type=int, default=1, help="evaluator processes splitting the workspaces")
    p.add_argument("--workspaces", type=int, default=200, help="same as workspaces.py; 0 = every ws-* topic in the stream")
    p.add_argument("--readers", type=int, default=2, help="connections reading this shard's partitions")
    p.add_argument("--quota", type=float, default=5_000, help="events a second each workspace may be evaluated at")
    p.add_argument("--budget-ms", type=float, default=100, help="p99 budget, event to evaluator")
    p.add_argument("--poll-ms", type=float, default=5, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
