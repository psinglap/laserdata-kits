"""The exchange side: every change in DSP appetite, written once to the lane.

Stands in for the part of the exchange that already sees every DSP response. For each (DSP, supply
segment) pair it keeps a recent bid rate and whether the DSP is bidding on that segment at all, and
writes a message whenever that changes. Most changes are small moves in the bid rate; a few each
second are flips, where a DSP stops bidding on a segment (a campaign or deal ends, a budget caps out,
a DSP drops a format) or starts again. Flips are what traffic shaping must see first: until a shaping
node knows, it keeps sending that DSP requests it will not bid on, or keeps holding back requests it
now wants.

One writer per partition, each with its own connection, writes every --tick-ms, so each pair's changes
stay in order. On start it writes one snapshot message per pair (sequence 1). After that it re-writes
every pair's current state once per --snapshot-every seconds, spread over the period, with the pair's
current sequence (live readers skip it as a repeat), so a shaping node that starts or restarts only
replays the last minute or so of the log to have every pair.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import APPETITE, DEFAULT_URL, PARTITIONS, STREAM, Window, connect, ensure_topic, msg, pairs, part


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, APPETITE)
    n_parts = PARTITIONS[APPETITE]
    keys = pairs(a.dsps, a.segments)
    by_part = [[k for k in keys if part(k) == p] for p in range(n_parts)]
    rng = random.Random(7)
    state = {}                                       # pair -> [bid rate, bidding?, seq]
    for k in keys:
        on = rng.random() < a.bidding
        state[k] = [rng.uniform(0.02, 0.4) if on else 0.0, on, 0]
    acks, sent, flips, t0 = Window(), [0], [0], time.monotonic()
    snapped = [0]
    gc.freeze()                                      # keep start-up tables out of the collector's way

    def change(k, flip=False, waited_ns=0, repeat=False):
        st = state[k]
        if not repeat:
            st[2] += 1
        return SendMessage(msg(k, st[2], st[0], st[1], flip, waited_ns))

    async def writer(p):
        c = await connect(a.url)
        mine = by_part[p]
        for i in range(0, len(mine), 5000):                          # snapshot: every pair, sequence 1
            await c.send_messages(STREAM, APPETITE, p, [change(k) for k in mine[i:i + 5000]])
        snapped[0] += 1
        await started.wait()
        share = len(mine) / len(keys)
        due, carry, fcarry, cursor, snap = time.monotonic(), 0.0, 0.0, 0, 0.0
        snap_per_write = len(mine) * a.tick_ms / (a.snapshot_every * 1000) if a.snapshot_every else 0
        while True:
            carry += a.rate * a.tick_ms / 1000 * share
            fcarry += a.flips * a.tick_ms / 1000 * share
            n, carry = int(carry), carry - int(carry)
            f, fcarry = int(fcarry), fcarry - int(fcarry)
            batch = []
            snap += snap_per_write                                   # rolling snapshot: a slice of pairs,
            if n or f or snap >= 10 * snap_per_write:                # riding on writes that carry live changes
                for _ in range(int(snap)):
                    batch.append(change(mine[cursor], repeat=True))
                    cursor = (cursor + 1) % len(mine)
                snap -= int(snap)
            for _ in range(n):                                       # bid-rate moves on pairs that are bidding
                for _ in range(10):
                    k = mine[int(len(mine) * rng.random() ** 2)]     # some pairs see far more traffic than others
                    st = state[k]
                    if st[1]:
                        break
                else:
                    continue
                st[0] = min(0.95, max(0.005, st[0] * rng.uniform(0.85, 1.15)))
                # A change happens at some point in the last --tick-ms and waits for this write; that wait counts.
                batch.append(change(k, waited_ns=int(rng.random() * a.tick_ms * 1e6)))
                sent[0] += 1
            for _ in range(f):                                       # a DSP stops or starts bidding on a segment
                k = mine[rng.randrange(len(mine))]
                st = state[k]
                st[1] = not st[1]
                st[0] = rng.uniform(0.05, 0.4) if st[1] else 0.0
                batch.append(change(k, flip=True, waited_ns=int(rng.random() * a.tick_ms * 1e6)))
                sent[0] += 1
                flips[0] += 1
            if batch:
                s0 = time.perf_counter()
                await c.send_messages(STREAM, APPETITE, p, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        await started.wait()
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[exchange] {sent[0]:>7,} appetite changes/s  flips {flips[0]:,}  {r['n']:,} writes  "
                  f"write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms", flush=True)
            sent[0] = flips[0] = 0

    started = asyncio.Event()

    async def go():
        nonlocal t0
        while snapped[0] < n_parts:
            await asyncio.sleep(0.05)
        on = sum(1 for s in state.values() if s[1])
        print(f"[exchange] snapshot: {len(keys):,} pairs ({a.dsps} DSPs x {a.segments:,} supply segments), "
              f"{on:,} bidding", flush=True)
        t0 = time.monotonic()
        started.set()

    await asyncio.gather(go(), report(), *(writer(p) for p in range(n_parts)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--dsps", type=int, default=60, help="demand partners the exchange calls out to")
    p.add_argument("--segments", type=int, default=5000, help="supply segments your shaping model keys on")
    p.add_argument("--bidding", type=float, default=0.7, help="share of pairs where the DSP bids at start")
    p.add_argument("--rate", type=int, default=20_000, help="appetite changes per second, all pairs")
    p.add_argument("--flips", type=float, default=200, help="per second: a DSP stops or starts bidding on a segment")
    p.add_argument("--tick-ms", type=float, default=2, help="each writer writes once per this many ms")
    p.add_argument("--snapshot-every", type=float, default=60, help="seconds per rolling snapshot of every pair; 0 = off")
    asyncio.run(main(p.parse_args()))
