"""The failed-writes processor: reads captured events back and re-publishes them to MSK.

One shard of the retrier fleet: it owns the failed-writes partitions whose id % --shards ==
--shard and reads each of them directly (no consumer group, so no rebalances; offsets are kept
on the broker per partition, so a restarted retrier resumes where it stopped). For every event
it calls republish(), which here stands in for the produce to the main MSK topic of the event's
priority lane. A publish that fails again goes back on failed-writes with attempt + 1 after a
short backoff; after --max-attempts it goes to failed-writes-dlq (kept 7 days) for investigation.

Repeats are dropped and any gap in a writer's per-partition sequence is counted as lost. Prints a
line a second:
  * capture-to-retry latency (written to failed-writes until this retrier has it), PASS or FAIL
    against --budget-ms;
  * events re-published this second and, so far, the share re-published inside their lane's SLO
    counted from the moment the first publish failed (P10 has 5 s);
  * retries written back, events sent to the DLQ, events lost, the oldest event picked up.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, DLQ, DLQ_RETENTION, FAILED, PRIORITIES, STREAM, Window, connect, ensure_topic,
                    load, msg)


async def republish(events: list, retry_fail: float) -> list:
    """Stand-in for the produce to the main MSK topic of each event's lane. True = MSK acknowledged it.
    A payload MSK will never accept ("bad") always fails; any other retry fails with --retry-fail."""
    return [not e["bad"] and random.random() >= retry_fail for e in events]


async def main(a):
    name = f"retrier-{a.shard}"
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, FAILED):
        print(f"[{name}] waiting for the publisher to create '{FAILED}'…", flush=True)
        await asyncio.sleep(1)
    await ensure_topic(admin, STREAM, DLQ, 1, DLQ_RETENTION)
    parts = (await admin.get_topic(STREAM, FAILED)).partitions_count
    mine = [pid for pid in range(parts) if pid % a.shards == a.shard]
    writer = await connect(a.url)                         # write-backs and DLQ on their own connection
    budget_ns = a.budget_ms * 1_000_000
    lat, st, total = Window(), defaultdict(int), defaultdict(int)
    ontime, done = defaultdict(int), defaultdict(int)
    last_seq, wseq = {}, defaultdict(int)
    backoff = []                                          # (due, pid, event) waiting to be written back
    caught_up = [False]                                   # a new reader first replays what was written before it
    gc.freeze()                                           # keep startup tables out of the collector's way

    async def handle(pid, ms, now):
        fresh = []
        for m in ms:
            e = load(m.payload())
            k = (e["src"], pid)
            last = last_seq.get(k, 0)
            if e["seq"] <= last:
                st["repeats"] += 1                        # a repeat: re-publish it once
                continue
            if e["seq"] > last + 1:
                total["lost"] += e["seq"] - last - 1      # a gap in a writer's sequence: events missing
            last_seq[k] = e["seq"]
            age = now - e["ns"]
            if not caught_up[0]:
                st["replayed"] += 1                       # written before this retrier started: not live latency
                st["max_age"] = max(st["max_age"], age)
            else:
                lat.add(age / 1e6)
            st["oldest"] = max(st["oldest"], age)
            fresh.append(e)
        if not fresh:
            return
        oks = await republish(fresh, a.retry_fail)
        dead = []
        for e, ok in zip(fresh, oks):
            if ok:
                late = (time.time_ns() - e["t0"]) / 1e9 > PRIORITIES[e["p"]]
                ontime[e["p"], late] += 1
                done["ok"] += 1
                st["ok"] += 1
            elif e["a"] >= a.max_attempts:
                dead.append(e)
            else:                                         # back on failed-writes after a backoff
                due = time.monotonic() + a.backoff_ms * e["a"] / 1000
                backoff.append((due, pid, e))
        if dead:
            await writer.send_messages(STREAM, DLQ, Partitioning.partition_id(0),
                                       [SendMessage(msg(**{**e, "ns": time.time_ns()})) for e in dead])
            done["dlq"] += len(dead)

    async def write_back():
        while True:
            await asyncio.sleep(0.01)
            now = time.monotonic()
            due = [b for b in backoff if b[0] <= now]
            if not due:
                continue
            backoff[:] = [b for b in backoff if b[0] > now]
            batches = defaultdict(list)
            for _, pid, e in due:
                wseq[pid] += 1
                batches[pid].append(SendMessage(msg(**{**e, "ns": time.time_ns(), "a": e["a"] + 1, "src": name,
                                                       "seq": wseq[pid]})))
            await asyncio.gather(*(writer.send_messages(STREAM, FAILED, Partitioning.partition_id(pid), b)
                                   for pid, b in batches.items()))
            st["retried"] += len(due)

    async def reader(pids):
        c = await connect(a.url)
        consumers = {pid: Consumer.Single(f"{name}-p{pid}") for pid in pids}
        while True:
            more = False
            for pid in pids:
                # Next + auto-commit: the broker keeps this partition's offset, so a restart resumes here.
                ms = await c.poll_messages(STREAM, FAILED, consumer=consumers[pid], partition_id=pid, count=a.batch,
                                           auto_commit=True, polling_strategy=PollingStrategy.Next())
                if ms:
                    await handle(pid, ms, time.time_ns())
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def pct(lane):
        n = ontime[lane, False] + ontime[lane, True]
        return f"{100 * ontime[lane, False] / n:6.2f}%" if n else "    n/a"

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the replay is finished
                if st["replayed"] and st["max_age"] < budget_ns:
                    caught_up[0] = True
                print(f"[{name}] catching up: replayed {st['replayed']:,} earlier failed writes this second", flush=True)
                st.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{name}] {r['n']:>6,} failed writes/s picked up  capture-to-retry p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms:g}ms)  re-published "
                  f"{st['ok']:,}  on time P10 {pct('P10')} P20 {pct('P20')} P30 {pct('P30')}  retried {st['retried']:,}  "
                  f"oldest {st['oldest'] / 1e6:6.1f}ms  lost {total['lost']:,}  repeats dropped {st['repeats']:,}  "
                  f"total re-published {done['ok']:,} to DLQ {done['dlq']:,}", flush=True)
            st.clear()

    groups = [mine[r::a.readers] for r in range(a.readers)]
    print(f"[{name}] reading failed-writes partitions {mine} on {a.readers} connections", flush=True)
    await asyncio.gather(report(), write_back(), *(reader(g) for g in groups if g))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--shard", type=int, default=0)
    p.add_argument("--shards", type=int, default=1, help="retrier processes splitting the partitions")
    p.add_argument("--readers", type=int, default=2, help="connections reading this shard's partitions")
    p.add_argument("--retry-fail", type=float, default=0.02, help="share of re-publishes that fail again")
    p.add_argument("--max-attempts", type=int, default=5, help="attempts before an event goes to the DLQ")
    p.add_argument("--backoff-ms", type=float, default=100, help="wait before a retry, times the attempt number")
    p.add_argument("--budget-ms", type=float, default=100, help="p99 budget, capture to retrier")
    p.add_argument("--poll-ms", type=float, default=5, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
