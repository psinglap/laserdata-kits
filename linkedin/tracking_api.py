"""Tracking API's side: client batches of latency-sensitive tracking signals, written to the rt-signals lane.

Plays the ingestion service for the latency-sensitive category. Client SDKs batch a member's interactions into one
request; each request carries its shared context (platform, app version, session) once, and each signal in it
gets the member's next sequence number. Every 5 ms the signals of all requests that arrived go out as one batch per
partition, keyed by pseudonymized member ID, all partitions at once, and a request counts as acknowledged only
when the broker has confirmed its signals (not when they sit in a local buffer).

Prints a line a second: signals and requests written, request-to-broker-ack p50/p99, and running totals.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, PLATFORMS, RT, SIGNALS, STREAM, Window, ensure_topics, member, msg, part


async def main(a):
    async def own_client():                 # one connection for admin calls, one for the producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), RT)
    parts = PARTITIONS[RT]
    # A short linger: the per-partition sends of one tick go out together, not paced apart.
    cfg = DirectProducerConfig(batch_length=2000, linger_time=timedelta(microseconds=200))
    out = await (await own_client()).producer(STREAM, RT, topic_partitions_count=parts, mode=cfg)
    run = f"{time.time_ns():x}"             # readers reset their per-member state when a new run starts
    names, weights = zip(*SIGNALS)
    plats, pw = zip(*PLATFORMS)
    seq = defaultdict(int)
    ack, stats, total = Window(), defaultdict(int), defaultdict(int)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def request(now_ns):
        # One client request: one member, a few signals, the shared context sent once.
        mid = member(random.randrange(a.members))
        ctx = {"p": random.choices(plats, pw)[0], "v": "2026.10." + str(random.randint(1, 9)),
               "s": f"{random.getrandbits(32):08x}"}
        sigs = []
        for t in random.choices(names, weights, k=random.randint(1, 6)):
            seq[mid] += 1
            sigs.append(SendMessage(msg(ns=now_ns, run=run, m=mid, seq=seq[mid], t=t, p=ctx["p"], v=ctx["v"])))
        return mid, sigs

    t0 = time.monotonic()
    due = t0
    budget = 0.0                             # signals owed, so fractional rates add up over ticks
    async with out:
        nxt_report = t0 + 1
        while True:
            now = time.monotonic()
            t = now - t0
            peak = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
            budget += a.rate * peak * a.tick_ms / 1000
            batches, n_req, n_sig = defaultdict(list), 0, 0
            now_ns = time.time_ns()
            while budget >= 1:
                mid, sigs = request(now_ns)
                batches[part(mid, parts)].extend(sigs)
                budget -= len(sigs)
                n_req += 1
                n_sig += len(sigs)
            if batches:
                await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(p))
                                       for p, ms in batches.items()))
                done = (time.time_ns() - now_ns) / 1e6
                for _ in range(n_req):
                    ack.add(done)           # every request in this tick was acknowledged now
                stats["signals"] += n_sig
                stats["requests"] += n_req
            if time.monotonic() >= nxt_report:
                nxt_report += 1
                r = ack.flush()
                for k, v in stats.items():
                    total[k] += v
                print(f"[tracking-api] {stats['signals']:>7,} signals/s in {stats['requests']:,} requests  "
                      f"request-to-broker-ack p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  "
                      f"{'PEAK x%g  ' % peak if peak != 1 else ''}"
                      f"totals: signals {total['signals']:,}  requests acknowledged {total['requests']:,}", flush=True)
                stats.clear()
            due += a.tick_ms / 1000             # schedule from the due time, so a slow tick doesn't drift
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--members", type=int, default=1_000_000, help="distinct members sending signals")
    p.add_argument("--rate", type=float, default=20000, help="latency-sensitive signals per second")
    p.add_argument("--tick-ms", type=float, default=5)
    p.add_argument("--peak", type=float, default=1.0, help="rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0)
    p.add_argument("--peak-for", type=float, default=0)
    asyncio.run(main(p.parse_args()))
