"""The Event Publisher's capture path: every event whose MSK publish fails goes to the failed-writes topic.

Plays the publishing client Klaviyo's event-publisher post describes. Events arrive at --rate a
second and are published to the main MSK topics; that path is simulated here (healthy publishes
are only counted). A share of publishes fail: --blip-share in normal times (transient Kafka
timeouts), and --outage-share from --outage-at for --outage-for seconds (Klaviyo's own scenario is
"half of all events for 10-20 minutes"). Every failed event is written at once to the failed-writes
topic on Apache Iggy, keyed by profile, with its priority lane, the time it first failed and a
sequence number per partition, so the retrier can prove nothing went missing. A small share
(--reject-share) carries a payload MSK will never accept, to exercise the DLQ.

Prints a line a second: events published, failed writes captured, and how long the capture write
took to be acknowledged (it sits on the request path of a fire-and-forget API, so it must be short).
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS, FAILED, SHARES, STREAM, Window, connect, ensure_topic, msg, part


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, STREAM, FAILED, a.partitions)
    parts = (await c.get_topic(STREAM, FAILED)).partitions_count
    lanes, weights = zip(*SHARES)
    props = os.urandom(a.size // 2).hex()                # stands in for the event's properties
    seq = defaultdict(int)                               # partition -> last sequence this publisher wrote
    ack, stats, total = Window(), defaultdict(int), defaultdict(int)
    owed_all, owed_fail = 0.0, 0.0
    t0 = time.monotonic()
    gc.freeze()                                          # keep the collector from pausing the sender

    def share(now):
        return a.outage_share if a.outage_at <= now - t0 < a.outage_at + a.outage_for else a.blip_share

    async def run():
        nonlocal owed_all, owed_fail
        tick = time.monotonic()
        while not a.duration or time.monotonic() - t0 < a.duration:
            now = time.monotonic()
            owed_all += a.rate * a.tick_ms / 1000
            n = int(owed_all)
            owed_all -= n
            owed_fail += n * share(now)
            k = int(owed_fail)
            owed_fail -= k
            batches = defaultdict(list)
            ns = time.time_ns()
            for _ in range(k):
                lane = random.choices(lanes, weights)[0]
                profile = f"pf{random.randrange(a.profiles):09d}"
                pid = part(profile, parts)
                seq[pid] += 1
                total["id"] += 1
                bad = random.random() < a.reject_share
                total["rejected"] += bad
                event = random.choice(EVENTS[lane])
                batches[pid].append(SendMessage(msg(
                    ns=ns, t0=ns, id=f"{a.name}-{total['id']:x}", pf=profile, p=lane, e=event,
                    a=1, src=a.name, seq=seq[pid], why="timeout", bad=int(bad), props=props)))
            if batches:
                # One batch per partition, all sent at once on this publisher's connection.
                s = time.perf_counter()
                await asyncio.gather(*(c.send_messages(STREAM, FAILED, Partitioning.partition_id(pid), ms)
                                       for pid, ms in batches.items()))
                ack.add((time.perf_counter() - s) * 1000)
            stats["published"] += n
            stats["failed"] += k
            total["captured"] += k
            tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)   # never bank more than 100 ms
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = ack.flush()
            sh = share(time.monotonic())
            state = f"MSK timeouts {sh:.0%}" if sh == a.outage_share and a.outage_for else f"MSK blips {sh:.1%}"
            print(f"[{a.name}] {stats['published']:>7,} events/s published  {state}: {stats['failed']:>6,} failed writes "
                  f"captured  capture ack p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  total captured "
                  f"{total['captured']:,} (rejected payloads {total['rejected']:,})", flush=True)
            stats.clear()

    print(f"[{a.name}] publishing {a.rate:,.0f} events/s; failed writes go to '{FAILED}' ({parts} partitions)", flush=True)
    rep = asyncio.ensure_future(report())
    await run()                                          # returns only when --duration is set and over
    rep.cancel()
    print(f"[{a.name}] done after {a.duration:g} s: total captured {total['captured']:,} "
          f"(rejected payloads {total['rejected']:,})", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="pub-0", help="publisher name; run several with different names")
    p.add_argument("--rate", type=float, default=20_000,
                   help="events a second through this publisher; run several for the full 170K/s peak")
    p.add_argument("--blip-share", type=float, default=0.001, help="share of publishes failing in normal times")
    p.add_argument("--outage-at", type=float, default=60, help="seconds after start the MSK timeouts begin")
    p.add_argument("--outage-for", type=float, default=0, help="how long they last (0 = never)")
    p.add_argument("--outage-share", type=float, default=0.5, help="share of publishes failing during them")
    p.add_argument("--reject-share", type=float, default=0.001, help="share of failed events MSK will never accept")
    p.add_argument("--profiles", type=int, default=50_000_000)
    p.add_argument("--partitions", type=int, default=16, help="partitions for a new failed-writes topic")
    p.add_argument("--size", type=int, default=400, help="bytes of event properties")
    p.add_argument("--tick-ms", type=float, default=20)
    p.add_argument("--duration", type=float, default=0, help="stop after this many seconds (0 = run until stopped)")
    asyncio.run(main(p.parse_args()))
