"""The uplink: forward the building's carton events to AWS, and hold them in the site log while the link is down.

Reads the site lane directly, partition by partition, on its own connection to the building's broker, at its own
pace, and sends every event, unchanged, to the cloud topic keyed by site:carton, so a carton's events stay in
order in AWS too. The floor view (site_state.py) reads the same lane on its own connections and never waits on
any of this.

--wan-down-at / --wan-down-for take the building's link to AWS down for a while (simulated here: the uplink's
sends are refused). A failed send to the cloud broker is handled the same way. The uplink keeps the unsent batch,
retries once a second and stops reading, so the events wait in the site log, on disk, not in memory and not on
the handhelds. When the link is back it drains the backlog from its own position, in order per carton, at up to
--wan-eps events a second. Events that wait out an outage are marked held and counted as held, never as live
latency, here and in AWS.

Prints, every second: events forwarded, floor-to-cloud p50/p99 while live, and, through an outage, how long the
link has been down and how many events are held in the building, then how long the backlog took to drain.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, Partitioning, PollingStrategy, SendMessage

from common import (CLOUD, CLOUD_EVENTS, DEFAULT_URL, EVENTS, FC, PARTITIONS, Window, connect, ensure_topics, load,
                    part)


class Link:
    """The building's link to AWS. Simulated outage: sends are refused between --wan-down-at and its end."""

    def __init__(self, a):
        self.a, self.t0 = a, time.monotonic()

    def down(self):
        t = time.monotonic() - self.t0
        return self.a.wan_down_for > 0 and self.a.wan_down_at <= t < self.a.wan_down_at + self.a.wan_down_for


async def main(a):
    site = await connect(a.url)                         # the building's broker: reading only
    await ensure_topics(site, EVENTS)
    cloud = await connect(a.cloud_url or a.url)         # the AWS broker: its own connection
    await ensure_topics(cloud, CLOUD_EVENTS)
    cfg = DirectProducerConfig(batch_length=5000, linger_time=timedelta(microseconds=200))
    out = await cloud.producer(CLOUD, CLOUD_EVENTS, topic_partitions_count=PARTITIONS[CLOUD_EVENTS], mode=cfg)
    link = Link(a)
    who = Consumer.Single(f"uplink-{os.getpid()}")
    parts = list(range((await site.get_topic(FC, EVENTS)).partitions_count))
    lat, stats = Window(), defaultdict(float)
    pending = []                            # (cloud partition, payload, ns) read from the site log, not yet in AWS
    st = {"mode": "catching up", "down_since": None, "held": 0, "held_s": 0.0, "drain_t0": None, "outages": 0}
    tot = defaultdict(int)
    gc.freeze()

    async def read():
        nxt = {}
        while True:
            if len(pending) >= a.max_pending:
                await asyncio.sleep(0.005)  # the link is down or behind: stop reading, events wait in the site log
                continue
            more = False
            for p in parts:
                ms = await site.poll_messages(FC, EVENTS, consumer=who, partition_id=p, count=a.batch,
                                              auto_commit=False,
                                              polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                              else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        b = m.payload()
                        e = load(b)
                        pending.append((part(f"{e['site']}:{e['carton']}", PARTITIONS[CLOUD_EVENTS]), b, e["ns"]))
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def forward():
        async with out:
            while True:
                await asyncio.sleep(a.flush_ms / 1000)
                if not pending:
                    continue
                batch = pending[:max(1, int(a.wan_eps * a.flush_ms / 1000))]
                # Events that waited out an outage (or a start-up replay) are marked held, so AWS never counts
                # them as live latency.
                held = st["down_since"] is not None or st["mode"] != "live"
                groups = defaultdict(list)
                for q, b, _ in batch:
                    groups[q].append(SendMessage(b[:-1] + b',"held":1}' if held else b))
                try:
                    if link.down():
                        raise ConnectionError("link to AWS down")
                    # One batch per cloud partition, all sent at once; a carton's events keep their order.
                    await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in groups.items()))
                except Exception:
                    if st["down_since"] is None:
                        st["down_since"] = time.monotonic()
                        st["outages"] += 1
                    await asyncio.sleep(1)  # keep the batch and retry; nothing is dropped or skipped
                    continue
                done = time.time_ns()
                if st["down_since"] is not None:            # the link is back: drain what waited in the building
                    st["down_since"] = None
                    st["mode"], st["drain_t0"], st["held"] = "draining", time.monotonic(), 0
                    st["held_s"] = (done - batch[0][2]) / 1e9   # how far behind the cloud was when the link came back
                del pending[:len(batch)]
                for _, _, ns in batch:
                    if st["mode"] == "live":
                        lat.add((done - ns) / 1e6)
                    elif st["mode"] == "draining":
                        st["held"] += 1             # waited out the outage in the site log: held, not live
                stats["sent"] += len(batch)
                stats["oldest"] = max(stats["oldest"], (done - batch[0][2]) / 1e9)
                tot["sent"] += len(batch)

    async def report():
        while True:
            await asyncio.sleep(1)
            if st["down_since"] is not None:
                print(f"[uplink] link to AWS down for {time.monotonic() - st['down_since']:4.1f}s: the floor keeps "
                      f"going, {len(pending):,} events read and waiting, the rest held in the site log  "
                      f"forwarded {tot['sent']:,}", flush=True)
            elif st["mode"] == "live":
                r = lat.flush()
                print(f"[uplink] live: forwarded {int(stats['sent']):>6,} events/s  floor-to-cloud p50 "
                      f"{r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  forwarded {tot['sent']:,}", flush=True)
            else:
                print(f"[uplink] {st['mode']}: forwarded {int(stats['sent']):,} this second, oldest "
                      f"{stats['oldest']:5.1f}s old  forwarded {tot['sent']:,}", flush=True)
                if stats["sent"] and stats["oldest"] < a.live_s:
                    if st["mode"] == "draining":
                        took = time.monotonic() - st["drain_t0"]
                        print(f"[uplink] link back: {st['held']:,} carton events ({st['held_s']:.1f}s of the floor) "
                              f"held in the building and drained to AWS in {took:.1f}s, in order", flush=True)
                    st["mode"] = "live"
                    lat.flush()
            stats.clear()

    await asyncio.gather(read(), forward(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL, help="the building's broker")
    p.add_argument("--cloud-url", default="", help="the AWS broker (default: the same broker)")
    p.add_argument("--batch", type=int, default=2000, help="events per poll, per partition")
    p.add_argument("--poll-ms", type=float, default=2)
    p.add_argument("--flush-ms", type=int, default=20, help="one send to AWS at most this often")
    p.add_argument("--wan-eps", type=float, default=20000, help="events a second the link carries, at most")
    p.add_argument("--max-pending", type=int, default=5000, help="stop reading above this many unsent events")
    p.add_argument("--live-s", type=float, default=1.0, help="back to live once the oldest forwarded event is younger")
    p.add_argument("--wan-down-at", type=float, default=30, help="the link drops this many seconds after start")
    p.add_argument("--wan-down-for", type=float, default=0, help="seconds; 0 = no outage")
    asyncio.run(main(p.parse_args()))
