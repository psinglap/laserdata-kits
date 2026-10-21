"""The alert service: every threshold crossing from the allowance tracker, as soon as it is published.

Reads usage-alerts directly, partition by partition, on its own connection, from its own position (no
consumer group, nothing stored on the broker per poll). In production this is where the usage alert emails
to app contributors and billing admins go out, and where a budget crossing could switch an app into a
reduced mode; here it records each alert and how long it took.

Alerts arrive at least once (a restarted tracker republishes what it rebuilds from the log), so each
(cycle, app, level) is acted on once and repeats are dropped.

Prints alerts per second by level, usage-to-alert p50/p99 (from the usage record that crossed the level to
the alert service holding the alert, both hops) and tracker-to-alert p50/p99 (the second hop alone).
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import ALERTS, BUDGET_AT, DEFAULT_URL, STREAM, Window, ensure_topics, load


async def main(a):
    c = IggyClient.from_connection_string(a.url)          # one connection for this reader
    await c.connect()
    await ensure_topics(c, ALERTS)
    start_ns = time.time_ns()
    e2e, hop = Window(), Window()
    seen = set()                            # (cycle, app, level) already acted on
    by_level = defaultdict(int)             # (cycle, level) -> alerts acted on
    stats = defaultdict(int)
    gc.freeze()

    def on_alert(m):
        d = load(m.payload())
        k = (d["c"], d["a"], d["lvl"])
        if k in seen:
            stats["dups"] += 1
            return
        seen.add(k)
        by_level[(d["c"], d["lvl"])] += 1
        stats["alerts"] += 1
        if d["ns"] > start_ns:              # live alerts only, not ones replayed at start-up
            now = time.time_ns()
            e2e.add((now - d["uns"]) / 1e6)
            hop.add((now - d["ns"]) / 1e6)
        stats["cycle"] = max(stats["cycle"], d["c"])

    async def read():
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        parts = list(range((await c.get_topic(STREAM, ALERTS)).partitions_count))
        nxt = {}
        while True:
            more = False
            for p in parts:
                ms = await c.poll_messages(STREAM, ALERTS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_alert(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            r, h = e2e.flush(), hop.flush()
            cy = stats["cycle"]
            lv = "  ".join(f"{x}% {by_level[(cy, x)]:,}" for x in (50, 75, 90, 100))
            print(f"[{a.name}] alerts {stats['alerts']:>5,}/s  usage-to-alert n {r['n']:,} p50 {r['p50']:.1f}ms "
                  f"p99 {r['p99']:.1f}ms  tracker-to-alert p50 {h['p50']:.1f}ms p99 {h['p99']:.1f}ms  "
                  f"cycle {cy % 10000}: {lv}  budget {by_level[(cy, BUDGET_AT)]:,}  repeats dropped {stats['dups']:,}",
                  flush=True)
            stats["alerts"] = 0
            for k in [k for k in seen if k[0] <= cy - 2]:
                seen.discard(k)

    await asyncio.gather(read(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="alerts")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--poll-ms", type=float, default=1, help="wait between polls when nothing more is waiting")
    asyncio.run(main(p.parse_args()))
