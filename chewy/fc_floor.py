"""The fulfillment center floor: every carton's events onto the site lane, on the building's broker.

Plays the WMS, the warehouse control system and the associates' handhelds of one building. Every carton goes
through the same six events, in order: PICKED (handheld), PACKED (pack station), LABELED (print-and-apply),
SORTED (sorter scan, with the dock door it was diverted to), LOADED (trailer handheld) and SHIP_CONFIRMED (WMS).
Each event is one message keyed by carton, with a per-carton sequence (1 to 6) and the carton's resulting step,
door and order, never a delta, so a reader that sees the newest event knows the carton's whole state.

At-least-once on purpose: --resend-pct of events are sent a second time, as a handheld that timed out waiting
for an acknowledgement would. Every reader must apply each one once.

The life of a carton is compressed (--step-min to --step-max seconds between events) so a one-minute run sees
thousands of cartons finish. Prints events, cartons started and ship-confirmed, and resends per second.
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Partitioning, SendMessage

from common import (DEFAULT_URL, DONE, EVENTS, FC, PARTITIONS, STEPS, carton_id, connect, ensure_topics, msg,
                    part)

TICK = 0.01
DEVICE = {"PICKED": "handheld", "PACKED": "pack-station", "LABELED": "print-and-apply", "SORTED": "sorter-scanner",
          "LOADED": "trailer-handheld", "SHIP_CONFIRMED": "wms"}


async def main(a):
    client = await connect(a.url)                       # one connection for this producer
    await ensure_topics(client, EVENTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await client.producer(FC, EVENTS, topic_partitions_count=PARTITIONS[EVENTS], mode=cfg)
    due = []                                            # (when the carton's next event is due, carton number)
    cartons = {}                                        # carton number -> [id, order, door, seq so far, partition]
    recent = deque(maxlen=4000)                         # recently sent events, for the at-least-once resends
    stats = defaultdict(float)
    n = [0]
    gc.freeze()                                         # keep start-up objects out of the collector's way

    async def floor():
        t0 = time.monotonic()
        nxt, owed = t0, 0.0
        async with producer:
            while True:
                now_m = time.monotonic()
                t = now_m - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                stats["peak"] = x > 1
                owed += a.cartons_per_s * x * TICK
                k, owed = int(owed), owed - int(owed)
                for _ in range(k):                      # new cartons: their first pick is due now
                    n[0] += 1
                    cid = carton_id(a.site, n[0])
                    cartons[n[0]] = [cid, f"ORD-{random.randrange(10**9):09d}", random.randint(1, a.doors), 0,
                                     part(cid, PARTITIONS[EVENTS])]
                    heapq.heappush(due, (now_m, n[0]))
                    stats["started"] += 1
                now = time.time_ns()
                batches = defaultdict(list)
                while due and due[0][0] <= now_m:
                    _, c = heapq.heappop(due)
                    cid, order, door, seq, pid = cartons[c]
                    seq += 1
                    step = STEPS[seq - 1]
                    b = msg(site=a.site, carton=cid, order=order, seq=seq, step=step, dev=DEVICE[step],
                            door=door if seq >= 4 else None, ns=now)
                    batches[pid].append(SendMessage(b))
                    stats["sent"] += 1
                    if seq == DONE:
                        del cartons[c]
                        stats["confirmed"] += 1
                    else:
                        cartons[c][3] = seq
                        heapq.heappush(due, (now_m + random.uniform(a.step_min, a.step_max), c))
                    if a.resend_pct and random.random() < a.resend_pct / 100 and recent:
                        q, old = random.choice(recent)  # a handheld retry of an earlier event: same carton, same seq
                        batches[q].append(SendMessage(old))
                        stats["resent"] += 1
                    recent.append((pid, b))
                # One batch per partition, all sent at once: a carton's events stay in order, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                nxt += TICK
                await asyncio.sleep(max(0.0, nxt - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "peak" if stats["peak"] else "steady"
            print(f"[floor {a.site}] {int(stats['sent']):>6,} carton events/s ({phase})  cartons started "
                  f"{int(stats['started']):,}  ship-confirmed {int(stats['confirmed']):,}  on the floor "
                  f"{len(cartons):,}  resent {int(stats['resent']):,}", flush=True)
            for k in ("sent", "started", "confirmed", "resent"):
                stats[k] = 0

    await asyncio.gather(floor(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL, help="the building's broker")
    p.add_argument("--site", default="AVP2")
    p.add_argument("--cartons-per-s", type=float, default=20, help="new cartons a second in this building")
    p.add_argument("--doors", type=int, default=60, help="outbound dock doors")
    p.add_argument("--step-min", type=float, default=1.0, help="seconds between a carton's events, at least")
    p.add_argument("--step-max", type=float, default=3.0, help="seconds between a carton's events, at most")
    p.add_argument("--resend-pct", type=float, default=0.5, help="share of events sent twice (handheld retries)")
    p.add_argument("--peak", type=float, default=1, help="carton-rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
