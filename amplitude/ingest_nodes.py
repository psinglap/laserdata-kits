"""A fleet of ingestion nodes behind one load balancer: accepted event counts onto the lane, throttles applied
before a device's next request.

Plays --nodes ingestion nodes in one process, and the SDKs that send to them: --devices devices sending
--events-per-s events a second in total (about what people produce), plus --spam-devices devices with broken
instrumentation that start one after another and send 40-300 events a second each, the rates Amplitude's docs
call spammy. Each request from a device lands on a random node, as behind a load balancer, so no node sees
more than its share of any device.

For every request a node accepts, it publishes to the device-counts topic, keyed by device, once per 10 ms tick:
  * node, device, the second, and the node's running count of accepted events for that device in that second
    (the resulting count, never only the increment), with a per-(node, device) sequence.
Each node also reads throttle-state directly on its own connection and applies it to its own view: a request
from a throttled device gets a 429 with throttled_devices and eps_threshold, and the SDK pauses that device for
--backoff-s seconds, as the docs ask.

Prints events accepted and refused per second, throttle-to-ingestion-node p50/p99 (the second hop), human
devices wrongly throttled, and how many throttles a node counting only its own traffic would have caught.
"""
import argparse
import asyncio
import gc
import json
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import (Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage)

from common import (BACKOFF_S, COUNTS, DEFAULT_URL, PARTITIONS, STATE, STREAM, THRESHOLD_EPS, WINDOW_S, Rate,
                    Window, device, ensure_topics, load, msg, part)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), COUNTS, STATE)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, COUNTS, topic_partitions_count=PARTITIONS[COUNTS], mode=cfg)
    start_ns = time.time_ns()
    nodes = [f"ingest-{i:02d}" for i in range(a.nodes)]
    devices = [device(i) for i in range(a.devices)]
    pid = {d: part(d, PARTITIONS[COUNTS]) for d in devices}
    rnd = random.Random(a.seed)
    spam = {}                               # device -> (start, events/s): broken instrumentation, one after another
    for i, d in enumerate(rnd.sample(devices, a.spam_devices)):
        spam[d] = (a.spam_at + i * a.spam_over / max(1, a.spam_devices), rnd.uniform(a.spam_low, a.spam_high))
    humans = [d for d in devices if d not in spam]
    view = {n: {} for n in nodes}           # node -> {device: (seq, state, eps)}
    local = {n: defaultdict(Rate) for n in nodes}   # node -> spam device -> what this node alone has accepted
    backoff = {}                            # device -> monotonic time its SDK resumes after a 429
    seq = defaultdict(int)                  # (node, device) -> sequence
    cur = {}                                # (node, device) -> [second, running count]
    lat, stats = Window(), defaultdict(float)
    example = [None]

    def reader(n):
        def on_state(m):
            s = load(m.payload())
            v = view[n].get(s["d"])
            if v and s["q"] <= v[0]:
                return                      # older or repeated decision: the newest wins
            view[n][s["d"]] = (s["q"], s["st"], s["eps"])
            if s["ns"] <= start_ns:
                return                      # replayed at start-up: applied, not timed
            lat.add((time.time_ns() - s["ns"]) / 1e6)
            if n == nodes[0] and s["st"] == "throttled":
                stats["throttles"] += 1
                if s["d"] not in spam:
                    stats["wrong"] += 1     # a device sending at human rates was throttled
                # The same moment, judged on what each node had accepted on its own: would any one node,
                # counting only its own share of the device's traffic, have crossed the threshold?
                sec = int(time.time())
                if any(local[x][s["d"]].avg(sec, WINDOW_S) >= a.threshold for x in nodes if s["d"] in local[x]):
                    stats["local"] += 1
        return on_state

    async def read_state(n):
        # Direct reads of the one throttle-state partition from the start of the log (every node rebuilds its
        # view from it), then from its own position; nothing is stored on the broker.
        c = await own_client()
        who, nxt, cb = Consumer.Single(f"{n}-{os.getpid()}"), None, reader(n)
        while True:
            ms = await c.poll_messages(STREAM, STATE, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    cb(m)
            if len(ms) < 1000:
                await asyncio.sleep(0.001)

    def throttled(n, d):
        v = view[n].get(d)
        return bool(v) and v[1] == "throttled"

    async def request_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        spam_owed = defaultdict(float)
        async with producer:
            while True:
                now = time.monotonic()
                t = now - t0
                sec = int(time.time())
                owed += a.events_per_s / 100
                reqs = defaultdict(int)     # device -> events in this tick's request
                for _ in range(int(owed)):
                    reqs[rnd.choice(humans)] += 1
                owed -= int(owed)
                for d, (at, eps) in spam.items():
                    if t >= at:
                        spam_owed[d] += eps / 100
                        k = int(spam_owed[d])
                        if k:
                            reqs[d] += k
                            spam_owed[d] -= k
                out = {}
                for d, k in reqs.items():
                    if backoff.get(d, 0) > now:
                        stats["held"] += k  # the SDK is pausing this device after a 429
                        continue
                    n = rnd.choice(nodes)
                    if throttled(n, d):
                        stats["refused"] += k
                        stats["r429"] += 1
                        backoff[d] = now + a.backoff_s
                        if example[0] is None:
                            example[0] = {"code": 429, "error": "Too many requests for some devices and users",
                                          "eps_threshold": a.threshold, "throttled_devices": {d: view[n][d][2]}}
                        continue
                    stats["accepted"] += k
                    if d in spam:
                        local[n][d].add(sec, k)
                    c = cur.get((n, d))
                    if not c or c[0] != sec:
                        c = cur[(n, d)] = [sec, 0]
                    c[1] += k
                    out[(n, d)] = c[1]
                batches = defaultdict(list)
                for (n, d), cnt in out.items():
                    seq[(n, d)] += 1
                    batches[pid[d]].append(SendMessage(msg(n=n, d=d, s=sec, c=cnt, q=seq[(n, d)])))
                    stats["records"] += 1
                # One batch per partition, all sent at once: per-device order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            n_thr = sum(1 for v in view[nodes[0]].values() if v[1] == "throttled")
            print(f"[ingest] {int(stats['accepted']):>7,} events/s accepted  {int(stats['records']):,} count records/s  "
                  f"refused with 429 {int(stats['refused']):,} events ({int(stats['r429']):,} requests)  "
                  f"held by SDK backoff {int(stats['held']):,}  throttled in {nodes[0]}'s view {n_thr:,}  "
                  f"throttle-to-ingestion-nodes n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            if stats["throttles"]:
                print(f"[compare] throttles this second {int(stats['throttles'])}  human devices throttled "
                      f"{int(stats['wrong'])}  a node counting alone would have caught {int(stats['local'])} of "
                      f"{int(stats['throttles'])} by then", flush=True)
            for k in ("accepted", "records", "refused", "r429", "held", "throttles", "wrong", "local"):
                totals[k] += stats[k]
                stats[k] = 0
            if example[0] is not None and not shown[0]:
                print("[ingest] example 429 body: " + json.dumps(example[0]), flush=True)
                shown[0] = True
            if int(time.monotonic()) % 10 == 0:
                print(f"[totals] throttles {int(totals['throttles'])}  human devices throttled {int(totals['wrong'])}  "
                      f"caught by a node counting alone {int(totals['local'])}  events refused "
                      f"{int(totals['refused']):,}", flush=True)
                old = int(time.time()) - WINDOW_S - 2
                for k in [k for k, v in cur.items() if v[0] < old]:
                    del cur[k]

    totals, shown = defaultdict(float), [False]
    gc.freeze()                             # keep start-up objects out of the collector's way
    await asyncio.gather(request_loop(), report(), *(read_state(n) for n in nodes))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--nodes", type=int, default=8, help="ingestion nodes behind the load balancer, each with its own reader")
    p.add_argument("--devices", type=int, default=100_000)
    p.add_argument("--events-per-s", type=float, default=10_000, help="events a second from devices at human rates")
    p.add_argument("--spam-devices", type=int, default=60, help="devices with broken instrumentation over the run")
    p.add_argument("--spam-at", type=float, default=5, help="seconds before the first one starts")
    p.add_argument("--spam-over", type=float, default=120, help="seconds over which they start, one after another")
    p.add_argument("--spam-low", type=float, default=40, help="events/s, lowest spam device")
    p.add_argument("--spam-high", type=float, default=300, help="events/s, highest spam device")
    p.add_argument("--threshold", type=float, default=THRESHOLD_EPS, help="must match throttle.py")
    p.add_argument("--backoff-s", type=float, default=BACKOFF_S, help="SDK pause after a 429, per device")
    p.add_argument("--seed", type=int, default=7)
    asyncio.run(main(p.parse_args()))
