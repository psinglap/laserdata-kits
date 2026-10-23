"""The cloud ledger in AWS: every building's carton events, applied once and in order, with the history checked.

Plays the service in AWS that turns ship-confirms into order status for the customer and keeps each carton's
event history for tracing and reconciliation. Reads the cloud topic directly, partition by partition, on its own
connection(s) to the AWS broker. For every site:carton it applies each event once by its per-carton sequence:
  * a sequence already applied (a handheld retry forwarded as sent, or a replay) changes nothing;
  * a sequence that skips one is a gap: an event lost on the way from the building (there should be none).
An event the uplink marked held (it waited out a link outage in the building's log), or one that arrives more than
--live-ms after it was scanned, is counted as held and drained, never as live latency.

Prints every second: events applied, floor-to-AWS p50/p99 for live events, held events that arrived, and the
ledger: cartons, ship-confirmed, cartons whose history is complete and in order, retries absorbed and gaps.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import CLOUD, CLOUD_EVENTS, DEFAULT_URL, DONE, Window, connect, ensure_topics, load


async def main(a):
    url = a.cloud_url or a.url
    await ensure_topics(await connect(url), CLOUD_EVENTS)
    lat, stats = Window(), defaultdict(float)
    last = {}                               # site:carton -> sequence applied
    gapped = set()                          # cartons with an event missing in AWS
    tot = defaultdict(int)
    gc.freeze()

    def on_event(m):
        e = load(m.payload())
        age = (time.time_ns() - e["ns"]) / 1e6
        k, seq = f"{e['site']}:{e['carton']}", e["seq"]
        prev = last.get(k, 0)
        if seq <= prev:
            tot["absorbed"] += 1
            return
        if seq > prev + 1:
            tot["gaps"] += 1
            gapped.add(k)
        last[k] = seq
        if seq == DONE:
            tot["confirmed"] += 1
        if e.get("held") or age > a.live_ms:
            stats["held"] += 1              # waited in the building's log through an outage, or a replay
            stats["oldest"] = max(stats["oldest"], age / 1000)
        else:
            lat.add(age)

    async def read(pids):
        c = await connect(url)
        who = Consumer.Single(f"cloud-ledger-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(CLOUD, CLOUD_EVENTS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_event(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            held = (f"  held events arrived {int(stats['held']):,} (oldest {stats['oldest']:.1f}s)"
                    if stats["held"] else "")
            print(f"[cloud] {r['n']:>6,} live events/s  floor-to-AWS p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms"
                  f"{held}", flush=True)
            print(f"[ledger] cartons {len(last):,}  ship-confirmed in AWS {tot['confirmed']:,}  "
                  f"history complete and in order {len(last) - len(gapped):,}/{len(last):,}  "
                  f"retries absorbed {tot['absorbed']:,}  gaps {tot['gaps']:,}", flush=True)
            stats.clear()

    parts = list(range((await (await connect(url)).get_topic(CLOUD, CLOUD_EVENTS)).partitions_count))
    await asyncio.gather(report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--cloud-url", default="", help="the AWS broker (default: --url)")
    p.add_argument("--readers", type=int, default=1)
    p.add_argument("--poll-ms", type=float, default=2)
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--live-ms", type=float, default=1000, help="older on arrival than this = held, not live")
    asyncio.run(main(p.parse_args()))
