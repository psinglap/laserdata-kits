"""The freshness service heartbeat triggers ask before they page, fed by the ingest freshness lane.

Reads ingest-receipts and query-progress, each on its own connection and consumer group, and keeps for every
dataset two times: when the ingest service last enqueued a batch for it, and the newest of its batches that
Retriever has applied on every Kafka partition it went to (so it is queryable). Every --eval-every seconds,
it answers each dataset's heartbeat:

  fire  nothing enqueued for --heartbeat-s: the customer has gone quiet. Page.
  hold  batches are arriving, but none inside the window is queryable and one has waited over --behind-s:
        Retriever is behind. A heartbeat that only queries recent data would page here; hold it and tell
        the on-call team instead.
  ok    data is arriving and queryable.

Each verdict change goes to heartbeat-verdicts in one batch every 10 ms, from its own task, never from the
read loop, so ingest.py can check it against the truth. Receipts carry the dataset's sequence, so a
replayed or repeated receipt changes nothing, and a restarted service rebuilds every dataset from the
retained lane: it holds nothing that the log does not.

Prints receipt-to-freshness p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy, SendMessage

from common import (DEFAULT_URL, PARTITIONS, PROGRESS, RECEIPTS, STREAM, VERDICTS, Stalls, Window, ensure_topics,
                    load, msg)

SLICES = 10    # each evaluation pass is spread over 10 slices, so no single step holds the read loop


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), RECEIPTS, PROGRESS, VERDICTS)

    async def reader(topic):
        return await (await own_client()).consumer_group(
            f"{a.name}-{topic}-{os.getpid()}", STREAM, topic, polling_strategy=PollingStrategy.First(),  # rebuild
            batch_length=5000, auto_commit=AutoCommit.Interval(timedelta(seconds=1)),   # offsets once a second
            poll_interval=timedelta(microseconds=500))

    receipts, progress = await reader(RECEIPTS), await reader(PROGRESS)
    out = await (await own_client()).producer(STREAM, VERDICTS, topic_partitions_count=PARTITIONS[VERDICTS])

    last_seq, last_rx, queryable = {}, {}, {}   # dataset -> receipt sequence, last enqueued (ns), newest queryable (ns)
    pending = {}                                # dataset -> {Kafka partition: deque of (offset, enqueued ns)}
    applied = defaultdict(lambda: -1)           # Kafka partition -> highest offset any Retriever node has applied
    verdict, order = {}, []                     # dataset -> "ok" | "hold" | "fire"; every dataset seen
    outbox = []                                 # verdict changes waiting for the sender task
    w_ns, behind_ns = int(a.heartbeat_s * 1e9), int(a.behind_s * 1e9)
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    caught_up = [False]                         # a new reader first replays what is retained on the lane
    gc.freeze()                                 # keep start-up objects out of the collector's way

    def timed(d):
        age = (time.time_ns() - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                                   # still replaying: track the oldest message seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_receipt(m):
        d = load(m.payload())
        timed(d)
        k = d["ds"]
        if k not in last_seq:
            order.append(k)
            verdict[k] = "ok"
            queryable[k] = d["ns"]              # first sight: nothing older to wait for
        elif d["seq"] <= last_seq[k]:
            stats["dropped"] += 1               # older or repeated receipt: already applied
            return
        last_seq[k], last_rx[k] = d["seq"], d["ns"]
        stats["receipts"] += 1
        if d["off"] <= applied[d["kp"]]:
            queryable[k] = max(queryable[k], d["ns"])
        else:
            pending.setdefault(k, {}).setdefault(d["kp"], deque()).append((d["off"], d["ns"]))

    async def on_progress(m):
        d = load(m.payload())
        timed(d)
        stats["progress"] += 1
        if d["off"] > applied[d["kp"]]:
            applied[d["kp"]] = d["off"]

    def evaluate(keys, now):
        for k in keys:
            for kp, q in pending.get(k, {}).items():
                top = applied[kp]
                while q and q[0][0] <= top:     # Retriever has applied this batch: it is queryable
                    queryable[k] = max(queryable[k], q.popleft()[1])
            waiting = [q[0][1] for q in pending.get(k, {}).values() if q]
            if now - last_rx[k] > w_ns:
                v = "fire"                      # nothing enqueued inside the window
            elif now - queryable[k] > w_ns and waiting and now - min(waiting) > behind_ns:
                v = "hold"                      # enqueued, but Retriever has not applied it for a while
            else:
                v = "ok"
            if v != verdict[k]:
                verdict[k] = v
                outbox.append(SendMessage(msg(ds=k, v=v, last=last_rx[k], ns=now)))
                stats[v] += 1

    async def heartbeats():
        # Every dataset once per --eval-every, in SLICES steps, scheduled from the due time, not from "now".
        due, i = time.monotonic(), 0
        while True:
            due += a.eval_every / SLICES
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            if not caught_up[0] or not order:
                continue
            n = -(-len(order) // SLICES)
            evaluate(order[i:i + n], time.time_ns())
            i = 0 if i + n >= len(order) else i + n

    async def sender():
        async with out:
            while True:
                await asyncio.sleep(0.01)
                if outbox:
                    batch = outbox[:]
                    outbox.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report_line():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of messages arrived inside the budget; partitions interleave,
                # so one fresh message alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier messages this second, "
                      f"state rebuilt for {len(order):,} datasets", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            held = sum(1 for v in verdict.values() if v == "hold")
            firing = sum(1 for v in verdict.values() if v == "fire")
            print(f"[{a.name}] {r['n']:>7,} msgs/s  receipt-to-freshness p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  datasets {len(order):,}  heartbeats now: "
                  f"held {held:,}  firing {firing:,}  verdicts sent {int(stats['sent']):,}  "
                  f"{stalls.read()}", flush=True)
            stats.clear()

    await asyncio.gather(receipts.consume_messages(on_receipt, asyncio.Event()),
                         progress.consume_messages(on_progress, asyncio.Event()),
                         heartbeats(), sender(), report_line(), stalls.watch())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="freshness")
    p.add_argument("--budget-ms", type=int, default=50, help="receipt-to-freshness p99 budget for the lane")
    p.add_argument("--heartbeat-s", type=float, default=60, help="heartbeat window: no data for this long pages")
    p.add_argument("--behind-s", type=float, default=5,
                   help="a batch not queryable after this long means Retriever is behind")
    p.add_argument("--eval-every", type=float, default=1.0, help="seconds between heartbeat checks per dataset")
    asyncio.run(main(p.parse_args()))
