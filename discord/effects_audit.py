"""The effect side: reads every execution result the rules workers wrote and checks each user's view.

Stands in for what reads Osprey's output sinks (enforcement, the investigations store). It reads every
partition of the execution-results topic directly and, per user, keeps when the likely_spammer label was
added and by which worker. Prints a line a second:
  * results per second, verdicts rejected, labels added, and action-to-result latency (action written to
    its result read here, through the rules worker and its send batches);
  * messages let through after the label: a send_message from an already labelled user that was still
    allowed, because it was evaluated without the label (the outcome a user in that server would see:
    the spam stays up); 0 is the pass line;
  * users labelled twice, or by two workers: two workers evaluated one user's actions at once; 0 is the
    pass line.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, RESULTS_TOPIC, STREAM, Window, connect, ensure_topic, load


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, RESULTS_TOPIC, a.result_partitions)
    parts = (await admin.get_topic(STREAM, RESULTS_TOPIC)).partitions_count
    c = await connect(a.url)
    consumers = {p: Consumer.Single(f"effects-audit-p{p}") for p in range(parts)}
    nxt = {}
    labelled_at, labelled_by = {}, {}                     # user -> sequence of the labelling action, worker
    twice = set()
    lat = Window()
    st, total = defaultdict(int), defaultdict(int)
    gc.freeze()

    def check(r, now):
        u = r["u"]
        if r["lbl"]:
            if u in labelled_at:
                twice.add(u)                              # labelled again: another worker never saw the label
                labelled_at[u] = min(labelled_at[u], r["seq"])
            else:
                labelled_at[u], labelled_by[u] = r["seq"], r["w"]
            st["labels"] += 1
        elif r["a"] == "send_message" and r["v"] == "allow" and u in labelled_at and r["seq"] > labelled_at[u]:
            st["through"] += 1                            # evaluated without the label that was already added
            total["through"] += 1
        if u in labelled_by and labelled_by[u] != r["w"] and r["a"] == "send_message":
            twice.add(u)
        st["reject"] += r["v"] == "reject"
        lat.add((now - r["ns"]) / 1e6)
        st["results"] += 1

    async def read():
        while True:
            more = False
            for p in range(parts):
                start = PollingStrategy.Offset(nxt[p]) if p in nxt else PollingStrategy.First()
                ms = await c.poll_messages(STREAM, RESULTS_TOPIC, consumer=consumers[p], partition_id=p,
                                           count=a.batch, auto_commit=False, polling_strategy=start)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        check(load(m.payload()), now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            print(f"[effects] {st['results']:>6,} results/s  {st['reject']:>5,} rejected  {st['labels']:>4,} labels added  "
                  f"action-to-result p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"messages let through after the label {st['through']:,}/s ({total['through']:,} total)  "
                  f"users labelled twice or by 2+ workers {len(twice):,}  users labelled {len(labelled_at):,}",
                  flush=True)
            st.clear()

    print(f"[effects] reading '{STREAM}/{RESULTS_TOPIC}' ({parts} partitions)", flush=True)
    await asyncio.gather(read(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--result-partitions", type=int, default=16, help="partitions for a new execution-results topic")
    p.add_argument("--poll-ms", type=float, default=10, help="the effect side is not on the timed path")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
