"""Flipkart Ads serving and event collection: every billable view and click onto the lane, every budget stop
applied before the next ad is served.

Plays --servers ad servers in one process. Each one serves sponsored ads for campaigns it has not been told
to stop, and every billable event goes to billable-events, keyed by campaign:
  * campaign, canonical-able impression ID, event time, kind (a viewable impression on a CPM campaign, a click
    on a CPC campaign), cost in ₹, budget day, ad server.
Like the real firehose it is not clean: --dup-share of events are sent a second time 50-500 ms later (a network
retry, the ID in another source's format), --late-share arrive minutes after their event time (an app kept its
batch in the background), and --stale-share arrive more than an hour late. BCAP charges each one once and drops
the stale ones (bcap.py). Every 100 ms the collector also writes a watermark mark to every partition, so BCAP
always knows how far behind each partition is.

Each ad server reads budget-stops directly on its own connection and applies them to its own view: a stopped
campaign is not served again that day. An ad server never sees another's view, so a stop reaches every server
through the log. Prints events per second, ads withheld by stops, and stop-to-ad-servers p50/p99: the second hop.
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

from common import (BILLABLE, CPC_HIGH, CPC_LOW, CPM_HIGH, CPM_LOW, DEFAULT_URL, PARTITIONS, STOPS, STREAM,
                    Window, campaign, day, ensure_topics, is_cpc, load, msg, part)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), BILLABLE, STOPS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, BILLABLE, topic_partitions_count=PARTITIONS[BILLABLE],
                                                   mode=cfg)
    start_ns = time.time_ns()
    servers = [f"ads-{i:02d}" for i in range(a.servers)]
    camps = [campaign(i) for i in range(a.campaigns)]
    pid = {c: part(c, PARTITIONS[BILLABLE]) for c in camps}
    cpm = {c: random.uniform(CPM_LOW, CPM_HIGH) for c in camps}
    cpc = {c: random.uniform(CPC_LOW, CPC_HIGH) for c in camps}
    view = {s: {} for s in servers}         # ad server -> {campaign: (day, ns of the stop)}
    lat, stats = Window(), defaultdict(float)
    retries = []                            # (due, partition, payload): events to send a second time
    imp = [0]

    def stop_reader(s):
        async def run():
            c = await own_client()
            who, nxt = Consumer.Single(f"{s}-{os.getpid()}"), None
            while True:
                ms = await c.poll_messages(STREAM, STOPS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                           else PollingStrategy.First())
                if not ms:
                    await asyncio.sleep(0.001)
                    continue
                nxt = ms[-1].offset() + 1
                now = time.time_ns()
                for m in ms:
                    d = load(m.payload())
                    v = view[s].get(d["c"])
                    if v and d["ns"] <= v[1]:
                        continue            # older or repeated decision: the newest wins
                    view[s][d["c"]] = (d["day"], d["ns"])
                    if d["ns"] > start_ns and d["day"] == day(a.day_s):
                        lat.add((now - d["ns"]) / 1e6)   # live stops only, not ones replayed at start-up
        return run()

    def allowed(s, c, d):
        v = view[s].get(c)
        return not v or v[0] != d

    def event(c, s, d, now_ms):
        imp[0] += 1
        raw = f"imp-{os.getpid() % 1000:03d}-{imp[0]:010d}"
        r = random.random()
        if r < a.stale_share:
            et = now_ms - random.randint(3_700_000, 7_200_000)       # more than an hour late
        elif r < a.stale_share + a.late_share:
            et = now_ms - random.randint(30_000, 3_000_000)          # minutes late, inside the hour
        else:
            et = now_ms - random.randint(0, 800)                     # straight from the app
        if is_cpc(c):
            kind, cost = "click", cpc[c]
        else:
            kind, cost = "view", cpm[c] / 1000
        return raw, dict(c=c, id=raw, et=et, kind=kind, cost=round(cost, 4), day=d, srv=s)

    async def serve_loop():
        t0 = due = time.monotonic()
        owed, next_wm = 0.0, 0.0
        async with producer:
            while True:
                t = time.monotonic() - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.events_per_s * x / 100
                d = day(a.day_s)
                now_ms = time.time_ns() // 1_000_000
                batches = defaultdict(list)
                for _ in range(int(owed)):
                    s = random.choice(servers)
                    for _ in range(4):      # an ad slot for a campaign this server may still serve
                        c = random.choice(camps)
                        if allowed(s, c, d):
                            break
                        stats["withheld"] += 1
                    else:
                        continue
                    raw, e = event(c, s, d, now_ms)
                    batches[pid[c]].append(SendMessage(msg(**e)))
                    stats["events"] += 1
                    stats["inr"] += e["cost"]
                    if random.random() < a.dup_share:   # the same event again later, ID in another source's shape
                        e2 = dict(e, id=raw.upper().replace("-", "_"))
                        retries.append((time.monotonic() + random.uniform(0.05, 0.5), pid[c], e2))
                mono = time.monotonic()
                keep = []
                for r in retries:
                    if r[0] <= mono:
                        batches[r[1]].append(SendMessage(msg(**r[2])))
                        stats["resent"] += 1
                    else:
                        keep.append(r)
                retries[:] = keep
                if mono >= next_wm:         # watermark marks: every partition shows progress, busy or not
                    next_wm = mono + 0.1
                    for p in range(PARTITIONS[BILLABLE]):
                        batches[p].append(SendMessage(msg(wm=1)))
                owed -= int(owed)
                # One batch per partition, all sent at once: per-campaign order is kept, about one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            d = day(a.day_s)
            stopped = sum(1 for v in view[servers[0]].values() if v[0] == d)
            phase = "peak" if stats["peak"] else "steady"
            print(f"[ads] {int(stats['events']):>7,} billable events/s ({phase})  ₹{stats['inr']:,.2f}/s  "
                  f"resent {int(stats['resent']):,}  withheld by stops {int(stats['withheld']):,}  "
                  f"stopped in {servers[0]}'s view {stopped:,}  "
                  f"stop-to-ad-servers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("events", "inr", "withheld", "resent"):
                stats[k] = 0

    gc.freeze()                             # keep start-up objects out of the collector's way
    await asyncio.gather(serve_loop(), report(), *(stop_reader(s) for s in servers))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--servers", type=int, default=8, help="ad servers, each with its own stop reader")
    p.add_argument("--campaigns", type=int, default=10_000, help="must match bcap.py")
    p.add_argument("--events-per-s", type=float, default=10_000, help="billable events per second, all servers")
    p.add_argument("--day-s", type=float, default=60, help="seconds per compressed budget day; must match bcap.py")
    p.add_argument("--dup-share", type=float, default=0.005, help="events sent a second time")
    p.add_argument("--late-share", type=float, default=0.02, help="events arriving minutes after their event time")
    p.add_argument("--stale-share", type=float, default=0.001, help="events arriving more than an hour late")
    p.add_argument("--peak", type=float, default=1, help="event-rate multiplier during the peak (a sale's opening hour)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
