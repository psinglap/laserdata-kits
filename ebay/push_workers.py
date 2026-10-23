"""Push workers: unicast notifications and broadcast window tasks in, HTTP pushes out.

Plays --workers push worker instances in one process. Worker k owns the partitions p of unicast-notifications and
broadcast-windows with p % workers == k, reads them directly on its own connection (no consumer group: a group
reader visits one partition per poll, so a record waits for its partition's turn), and:
  * for a unicast notification, pushes it to that subscription's endpoint;
  * for a window task, takes the window's subscriptions from its own cache (task affinity: window w always lands on
    partition w % 16, so the same worker serves it every time) and pushes the notification to each of them.
The HTTP call itself is simulated: an endpoint acknowledges unless it is in one of the kit's planted outages
(common.endpoint_down). A push that is not acknowledged goes to push-retry, keyed by subscription, for
retry_worker.py; the window carries on without it. Each window sends a receipt to window-receipts, which the
event processor turns into broadcast completion time. Sends are queued and go out from their own tasks every few
milliseconds, so no read loop waits on a send.

Prints event-to-push p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, for windows and unicast
together, and unicast p99 on its own: the number that shows whether a broadcast holds anyone else up.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, PARTITIONS, RECEIPTS, RETRY, STREAM, TASKS, UNICAST, Window, endpoint_down,
                    ensure_topics, load, msg, part, sub_id, window_subs, windows)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), UNICAST, TASKS, RETRY, RECEIPTS)
    cfg = DirectProducerConfig(batch_length=5000, linger_time=timedelta(microseconds=200))
    retry_out = await (await own_client()).producer(STREAM, RETRY, topic_partitions_count=PARTITIONS[RETRY], mode=cfg)
    receipt_out = await (await own_client()).producer(STREAM, RECEIPTS, topic_partitions_count=PARTITIONS[RECEIPTS],
                                                      mode=cfg)
    lat, uni_lat = Window(), Window()
    stats = defaultdict(float)
    retries, receipts = [], []              # queued sends
    caught_up = [False]                     # a new reader first replays notifications from before it joined
    up_args = (a.down_share, a.up_s, a.down_s)

    def push(i, t):
        """Stands in for the HTTP POST to the subscription's endpoint: True if the endpoint acknowledged."""
        return not endpoint_down(i, t, *up_args)

    def worker(k, parts):
        cache = {}                          # window -> [(index, subscription id)], this worker's windows only
        n_win = windows(a.subs, a.window_size)
        for w in range(n_win):              # prefill: the windows this worker owns, before traffic arrives
            if w % PARTITIONS[TASKS] in parts:
                cache[(a.subs, a.window_size, w)] = [(i, sub_id(i)) for i in window_subs(w, a.subs, a.window_size)]
        seen = {}                           # subscription -> last unicast sequence applied
        done = deque(maxlen=100_000)        # (event, window) tasks already pushed, so a repeat is skipped
        done_set = set()

        def apply(age_ms, live):
            if caught_up[0]:
                if live:
                    lat.add(age_ms)
            else:
                stats["replayed"] += 1
                stats["max_age"] = max(stats["max_age"], age_ms)

        def on_unicast(d):
            s = d["s"]
            if d["seq"] <= seen.get(s, 0):
                stats["dupes"] += 1
                return
            seen[s] = d["seq"]
            now = time.time()
            if not push(d["i"], now):
                retries.append((s, msg(s=s, i=d["i"], e=f"{s}#{d['seq']}", attempt=1, first=d["ns"],
                                       due=time.time_ns() + int(a.backoff_s * 1e9))))
                stats["held"] += 1
            age = (time.time_ns() - d["ns"]) / 1e6
            apply(age, True)
            if caught_up[0]:
                uni_lat.add(age)
            stats["unicast"] += 1

        def on_task(d):
            tk = (d["e"], d["w"])
            if tk in done_set:
                stats["dupes"] += 1
                return
            if len(done) == done.maxlen:
                done_set.discard(done[0])
            done.append(tk)
            done_set.add(tk)
            ck = (d["subs"], d["size"], d["w"])
            subs = cache.get(ck)
            if subs is None:                # a window this worker had not cached (the topic grew): build it once
                stats["misses"] += 1
                subs = cache[ck] = [(i, sub_id(i)) for i in window_subs(d["w"], d["subs"], d["size"])]
            now = time.time()
            first_ns = time.time_ns()
            n = held = 0
            for i, s in subs:
                body = f'{{"topic":"{d["t"]}","event":"{d["e"]}","subscription":"{s}"}}'
                if push(i, now):
                    n += 1
                else:
                    held += 1
                    retries.append((s, msg(s=s, i=i, e=d["e"], attempt=1, first=first_ns, body=body,
                                           due=first_ns + int(a.backoff_s * 1e9))))
            t_done = time.time_ns()
            receipts.append(msg(e=d["e"], w=d["w"], n=n, held=held, done=t_done))
            apply((t_done - d["ns"]) / 1e6, True)
            stats["windows"] += 1
            stats["pushed"] += n
            stats["held"] += held

        async def read():
            c = await own_client()
            who = Consumer.Single(f"push-{k}-{os.getpid()}")
            nxt = {}
            plan = [(UNICAST, p, on_unicast) for p in parts] + [(TASKS, p, on_task) for p in parts]
            while True:
                more = False
                for topic, p, cb in plan:
                    pos = nxt.get((topic, p))
                    ms = await c.poll_messages(STREAM, topic, consumer=who, partition_id=p, count=a.batch,
                                               auto_commit=False,
                                               polling_strategy=PollingStrategy.Offset(pos) if pos is not None
                                               else PollingStrategy.First())
                    if ms:
                        nxt[(topic, p)] = ms[-1].offset() + 1
                        for m in ms:
                            cb(load(m.payload()))
                            if topic == TASKS:
                                await asyncio.sleep(0)   # after each window, let the other workers read
                        more = more or len(ms) == a.batch
                if not more:
                    await asyncio.sleep(a.poll_ms / 1000)
        return read

    async def send_retries():
        # Not-acknowledged pushes go to push-retry, one batch per partition every 5 ms, never from a read loop.
        async with retry_out:
            while True:
                await asyncio.sleep(0.005)
                if retries:
                    batch = retries[:]
                    retries.clear()
                    groups = defaultdict(list)
                    for s, b in batch:
                        groups[part(s, PARTITIONS[RETRY])].append(SendMessage(b))
                    await asyncio.gather(*(retry_out.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in groups.items()))

    async def send_receipts():
        async with receipt_out:
            while True:
                await asyncio.sleep(0.005)
                if receipts:
                    batch = [SendMessage(b) for b in receipts]
                    receipts.clear()
                    await receipt_out.send(batch)

    async def report():
        print(f"[push] {a.workers} push workers, {windows(a.subs, a.window_size)} windows of {a.window_size} cached "
              f"by owner, {a.down_share:.1%} of endpoints in planted outages ({a.down_s:g} s down, {a.up_s:g} s up)",
              flush=True)
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of notifications arrived inside the budget; partitions interleave,
                # so one fresh notification alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[push] catching up: replayed {int(stats['replayed']):,} earlier notifications and windows "
                      f"this second", flush=True)
                stats.clear()
                lat.flush()
                uni_lat.flush()
                continue
            r, u = lat.flush(), uni_lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[push] {int(stats['unicast']):>6,} unicast/s  {int(stats['windows']):>3} windows/s "
                  f"({int(stats['pushed']):,} pushed)  event-to-push p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  unicast p99 {u['p99']:.1f}ms  "
                  f"held for retry {int(stats['held']):,}  repeats skipped {int(stats['dupes'])}  "
                  f"cache misses {int(stats['misses'])}", flush=True)
            stats.clear()

    n_parts = PARTITIONS[TASKS]
    readers = [worker(k, [p for p in range(n_parts) if p % a.workers == k]) for k in range(a.workers)]
    gc.freeze()                             # keep start-up objects (and the cache) out of the collector's way
    await asyncio.gather(send_retries(), send_receipts(), report(), *(r() for r in readers))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--workers", type=int, default=4, help="push worker instances, each owning 16/workers partitions")
    p.add_argument("--subs", type=int, default=20_000, help="subscriptions on the broadcast topic, for the cache")
    p.add_argument("--window-size", type=int, default=200)
    p.add_argument("--budget-ms", type=int, default=50, help="event-to-push p99 budget for the lane")
    p.add_argument("--down-share", type=float, default=0.01, help="share of endpoints in planted outages")
    p.add_argument("--down-s", type=float, default=6, help="seconds an endpoint in an outage stops acknowledging")
    p.add_argument("--up-s", type=float, default=14, help="seconds it acknowledges between outages")
    p.add_argument("--backoff-s", type=float, default=1, help="first retry delay")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=2000)
    asyncio.run(main(p.parse_args()))
