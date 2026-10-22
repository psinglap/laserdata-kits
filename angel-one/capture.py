"""The price capture engine: every price change for every scrip, written once to the candle lane.

Stands in for the on-premise price capture that writes the exchange price stream into Kafka
today. One writer per partition, each with its own connection, writing once per --write-ms;
scrips are spread over the partitions by part(), so every scrip's ticks stay in order. A tick
carries the resulting state (price, total volume for the day), never a delta, and the time the
capture engine stamped it, so its wait for the next write counts in the delay.
--burst multiplies the tick rate for --burst-for seconds, as at the 09:15 open.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, PARTITIONS, STREAM, TICKS, Window, connect, ensure_topic, pack, part, popular, scrips


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, TICKS, PARTITIONS)
    codes = scrips(a.scrips)
    by_part = [[s for s in codes if part(s) == p] for p in range(PARTITIONS)]
    rng = random.Random(7)
    price = {s: rng.randint(2_000, 400_000) * 5 for s in codes}      # paise, on a 5-paise tick size
    volume = dict.fromkeys(codes, 0)
    seq = dict.fromkeys(codes, 0)
    writers = [await connect(a.url) for _ in range(PARTITIONS)]
    acks, sent, t0 = Window(), [0], time.monotonic()
    gc.freeze()                                    # keep start-up tables out of the collector's way

    async def writer(p):
        c, mine = writers[p], by_part[p]
        due, carry, last = time.monotonic(), 0.0, time.time_ns()
        while True:
            el = time.monotonic() - t0
            x = a.burst if a.burst_at and a.burst_at <= el < a.burst_at + a.burst_for else 1
            carry += a.rate * x * a.write_ms / 1000 * len(mine) / len(codes)
            n, carry = int(carry), carry - int(carry)
            if n:
                now = time.time_ns()
                # Each price change happened at some point since the last write and waited for this one;
                # stamp it then, oldest first, so the wait counts and timestamps rise along the partition.
                span = min(a.write_ms * 1e6, now - last)
                waits = sorted((int(rng.random() * span) for _ in range(n)), reverse=True)
                last = now
                batch = []
                for w in waits:
                    s = mine[popular(rng, len(mine))]
                    price[s] = max(5, price[s] + rng.choice((-2, -1, -1, 1, 1, 2)) * 5)
                    volume[s] += rng.choice((1, 1, 5, 10, 25, 50, 75))
                    seq[s] += 1
                    batch.append(SendMessage(pack(s, now - w, price[s], volume[s], seq[s])))
                s0 = time.perf_counter()
                await c.send_messages(STREAM, TICKS, p, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
                sent[0] += n
            due += a.write_ms / 1000                # from the due time, so a slow write never drifts the schedule
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            el = time.monotonic() - t0
            burst = f"  burst {a.burst:g}x" if a.burst_at and a.burst_at <= el < a.burst_at + a.burst_for else ""
            print(f"[capture] {sent[0]:>7,} ticks/s  {r['n']:,} writes  write ack p50 {r['p50']:.1f}ms  "
                  f"p99 {r['p99']:.1f}ms{burst}", flush=True)
            sent[0] = 0

    print(f"[capture] {len(codes):,} scrips over {PARTITIONS} partitions, {a.rate:,} ticks/s", flush=True)
    await asyncio.gather(report(), *(writer(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--scrips", type=int, default=40_000)
    p.add_argument("--rate", type=int, default=20_000, help="ticks per second, all scrips")
    p.add_argument("--write-ms", type=float, default=1, help="each writer writes once per this many ms")
    p.add_argument("--burst", type=float, default=1, help="tick-rate multiplier during the burst")
    p.add_argument("--burst-at", type=float, default=0, help="seconds after start; 0 = no burst")
    p.add_argument("--burst-for", type=float, default=10)
    asyncio.run(main(p.parse_args()))
