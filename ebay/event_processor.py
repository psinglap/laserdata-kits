"""The event processor: business events in, unicast notifications and broadcast window tasks out.

Plays the Event Subscription Platform's event processor. Every 10 ms it publishes the unicast notifications that
came due (--unicast-rate a second over --unicast-subs subscriptions, eBay Notification API topic names), keyed by
subscription, with a per-subscription sequence. Every --broadcast-every seconds a broadcast event arrives for
--subs subscriptions; the processor only splits it into windows of --window-size subscriptions (ordered by
creation time, as eBay's post describes) and submits one task per window to broadcast-windows, at partition
window % 16, so the push worker that owns a window always serves it from its cache. It never loads the
subscriptions itself, so a broadcast cannot hold the event thread.

It also reads window-receipts on its own connection, and prints the business number for each broadcast: the time
from the event until every window was pushed, with the pushes acknowledged on the first attempt and the ones held
for retry because their endpoint did not acknowledge.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (BROADCAST_TOPIC, DEFAULT_URL, PARTITIONS, RECEIPTS, STREAM, TASKS, UNICAST, UNICAST_TOPICS,
                    Window, ensure_topics, load, msg, part, sub_id, windows)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), UNICAST, TASKS, RECEIPTS)
    cfg = DirectProducerConfig(batch_length=5000, linger_time=timedelta(microseconds=200))
    uni = await (await own_client()).producer(STREAM, UNICAST, topic_partitions_count=PARTITIONS[UNICAST], mode=cfg)
    tasks = await (await own_client()).producer(STREAM, TASKS, topic_partitions_count=PARTITIONS[TASKS], mode=cfg)
    rnd = random.Random(11)
    seq = defaultdict(int)
    upid = {}
    n_win = windows(a.subs, a.window_size)
    pending = {}                            # broadcast event -> what the receipts have said so far
    done = Window()
    stats = defaultdict(float)
    run = f"{os.getpid():x}"                # event ids are unique per run, so old receipts are ignored
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def unicast_loop():
        tick = time.monotonic()
        owed = 0.0
        async with uni:
            while True:
                owed += a.unicast_rate * 0.01
                n, owed = int(owed), owed - int(owed)
                batches = defaultdict(list)
                for _ in range(n):
                    i = rnd.randrange(a.unicast_subs)
                    s = sub_id(i)
                    seq[s] += 1
                    p = upid.get(s)
                    if p is None:
                        p = upid[s] = part(s, PARTITIONS[UNICAST])
                    batches[p].append(SendMessage(msg(s=s, i=i, seq=seq[s], t=rnd.choice(UNICAST_TOPICS))))
                # One batch per partition, all sent at once: per-subscription order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(uni.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["unicast"] += n
                tick += 0.01                # scheduled from its due time, so a slow tick doesn't drift the rest
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def broadcast_loop():
        nxt = time.monotonic() + a.broadcast_at
        n = 0
        async with tasks:
            while True:
                await asyncio.sleep(max(0.0, nxt - time.monotonic()))
                nxt += a.broadcast_every
                n += 1
                e = f"evt-{run}-{n:06d}"
                ens = time.time_ns()
                pending[e] = {"ns": ens, "got": set(), "last": 0, "pushed": 0, "held": 0}
                # Split only: (event, window) tasks, window w to partition w % 16 (task affinity).
                batches = defaultdict(list)
                for w in range(n_win):
                    batches[w % PARTITIONS[TASKS]].append(SendMessage(msg(
                        e=e, w=w, subs=a.subs, size=a.window_size, t=BROADCAST_TOPIC, ns=ens)))
                await asyncio.gather(*(tasks.send_with_partitioning(ms, Partitioning.partition_id(p))
                                       for p, ms in batches.items()))
                stats["split_ms"] = max(stats["split_ms"], (time.time_ns() - ens) / 1e6)
                stats["broadcasts"] += 1

    def on_receipt(d):
        b = pending.get(d["e"])
        if b is None or d["w"] in b["got"]:
            return                          # another run's receipt, or a repeat: counted once
        b["got"].add(d["w"])
        b["last"] = max(b["last"], d["done"])
        b["pushed"] += d["n"]
        b["held"] += d["held"]
        if len(b["got"]) == n_win:
            ms = (b["last"] - b["ns"]) / 1e6
            done.add(ms)
            stats["completed"] += 1
            stats["late"] += ms > a.complete_budget_ms
            print(f"[esp] broadcast {d['e']} {BROADCAST_TOPIC}: {a.subs:,} subscriptions in {n_win} windows, "
                  f"all pushed +{ms:.0f} ms after the event (acknowledged first time {b['pushed']:,}, "
                  f"held for retry {b['held']:,})", flush=True)
            del pending[d["e"]]

    async def receipts():
        c = await own_client()
        who = Consumer.Single(f"esp-receipts-{os.getpid()}")
        nxt = None
        while True:
            ms = await c.poll_messages(STREAM, RECEIPTS, consumer=who, partition_id=0, count=2000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    on_receipt(load(m.payload()))
            else:
                await asyncio.sleep(0.005)

    async def report():
        print(f"[esp] {a.unicast_rate:,} unicast notifications/s over {a.unicast_subs:,} subscriptions; a broadcast "
              f"to {a.subs:,} subscriptions every {a.broadcast_every:g} s, split into {n_win} windows of "
              f"{a.window_size}", flush=True)
        while True:
            await asyncio.sleep(1)
            r = done.flush()
            now = time.time_ns()
            stuck = sum(1 for b in pending.values() if now - b["ns"] > 5e9)
            comp = (f"completion p50 {r['p50']:.0f}ms max {r['max']:.0f}ms" if r["n"] else "completion -")
            print(f"[esp] {int(stats['unicast']):>6,} unicast/s  broadcasts split {int(stats['broadcasts'])} "
                  f"(split+submit {stats['split_ms']:.1f}ms)  completed {int(stats['completed'])}, over "
                  f"{a.complete_budget_ms}ms {int(stats['late'])}  {comp}  open over 5 s {stuck}", flush=True)
            stats.clear()

    await asyncio.gather(unicast_loop(), broadcast_loop(), receipts(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--unicast-rate", type=int, default=3000, help="unicast notifications a second (illustrative)")
    p.add_argument("--unicast-subs", type=int, default=200_000, help="subscriptions that get unicast notifications")
    p.add_argument("--subs", type=int, default=20_000, help="subscriptions on the broadcast topic (eBay's post: 20,000)")
    p.add_argument("--window-size", type=int, default=200, help="subscriptions per window (eBay's post: 200)")
    p.add_argument("--broadcast-every", type=float, default=5, help="seconds between broadcast events")
    p.add_argument("--broadcast-at", type=float, default=3, help="seconds after start of the first broadcast")
    p.add_argument("--complete-budget-ms", type=int, default=500, help="time from event to every window pushed")
    asyncio.run(main(p.parse_args()))
