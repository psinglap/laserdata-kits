"""The exchange feed handlers: every tick for every instrument, written once to the tick lane.

One handler per exchange segment (NSE, NFO, BSE, MCX), each with its own connection, writing
to its own partition every --tick-ms, so every instrument's ticks stay in order. A tick is
Kite's 44-byte quote packet plus a per-instrument sequence number and the time it was created;
it carries the resulting quote (last price, volume, OHLC), never a delta, so any reader can
start from any tick.

On start it writes one snapshot tick per instrument (yesterday's close, sequence 1). After that
it re-writes every instrument's current quote once per --snapshot-every seconds, spread over
the period, with the instrument's current sequence (live readers skip it as a repeat). A ticker
node that restarts mid-session then only replays the last minute or so of the log to have a
quote for every instrument, traded recently or not.
--burst multiplies the tick rate for --burst-for seconds: "A 10x burst in traffic over a
period of 10 seconds is quite common on a trading platform" (Zerodha Tech Blog, 2020).
"""
import argparse
import asyncio
import random
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, SEGMENTS, STREAM, TICKS, Window, connect, ensure_topic, instruments, pack, popular, segment_of


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, TICKS, len(SEGMENTS))
    tokens = instruments(a.instruments)
    by_seg = [[t for t in tokens if segment_of(t) == s] for s in range(len(SEGMENTS))]
    rng = random.Random(7)
    close = {t: rng.randint(2_000, 400_000) * 5 for t in tokens}      # paise, on a 5-paise tick size
    q = {t: [close[t], 0, close[t], close[t], close[t]] for t in tokens}   # ltp, volume, open, high, low
    seq = dict.fromkeys(tokens, 0)
    acks, sent, t0 = Window(), [0], time.monotonic()

    def tick(t, last_qty=0, waited_ns=0, repeat=False):
        if not repeat:
            seq[t] += 1
        ltp, vol, o, h, l = q[t]
        return SendMessage(pack(t, ltp, last_qty, ltp, vol, vol // 2, vol // 3, o, h, l, close[t], seq[t], waited_ns))

    async def handler(s):
        c = await connect(a.url)
        mine = by_seg[s]
        for i in range(0, len(mine), 5000):                          # snapshot: yesterday's close
            await c.send_messages(STREAM, TICKS, s, [tick(t) for t in mine[i:i + 5000]])
        await started.wait()
        due, carry, cursor, snap = time.monotonic(), 0.0, 0, 0.0
        snap_per_write = len(mine) * a.tick_ms / (a.snapshot_every * 1000) if a.snapshot_every else 0
        while True:
            el = time.monotonic() - t0
            x = a.burst if a.burst_at and a.burst_at <= el < a.burst_at + a.burst_for else 1
            carry += a.rate * x * a.tick_ms / 1000 * len(mine) / len(tokens)   # busier segments tick more
            n, carry = int(carry), carry - int(carry)
            batch = []
            snap += snap_per_write                                   # rolling snapshot: a slice of instruments,
            if n or snap >= 10 * snap_per_write:                     # riding on writes that carry live ticks
                for _ in range(int(snap)):
                    batch.append(tick(mine[cursor], repeat=True))
                    cursor = (cursor + 1) % len(mine)
                snap -= int(snap)
            if n:
                for _ in range(n):
                    t = mine[popular(rng, len(mine))]
                    st = q[t]
                    st[0] = max(5, st[0] + rng.choice((-2, -1, -1, 1, 1, 2)) * 5)   # last price moves a few ticks
                    qty = rng.choice((1, 1, 5, 10, 25, 50, 75))
                    st[1] += qty
                    st[3], st[4] = max(st[3], st[0]), min(st[4], st[0])
                    # A tick arrives at some point in the last --tick-ms and waits for this write; that wait counts.
                    batch.append(tick(t, qty, int(rng.random() * a.tick_ms * 1e6)))
            if batch:
                s0 = time.perf_counter()
                await c.send_messages(STREAM, TICKS, s, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
                sent[0] += n
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        await started.wait()
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            el = time.monotonic() - t0
            burst = f"  burst {a.burst:g}x" if a.burst_at and a.burst_at <= el < a.burst_at + a.burst_for else ""
            print(f"[feed] {sent[0]:>7,} ticks/s  {r['n']:,} writes  write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms{burst}",
                  flush=True)
            sent[0] = 0

    started = asyncio.Event()

    async def go():
        nonlocal t0
        while sum(seq.values()) < len(tokens):
            await asyncio.sleep(0.05)
        print(f"[feed] snapshot: {len(tokens):,} instruments across {', '.join(SEGMENTS)}", flush=True)
        t0 = time.monotonic()
        started.set()

    await asyncio.gather(go(), report(), *(handler(s) for s in range(len(SEGMENTS))))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--instruments", type=int, default=60_000)
    p.add_argument("--rate", type=int, default=20_000, help="ticks per second, all segments")
    p.add_argument("--tick-ms", type=float, default=1, help="each handler writes once per this many ms")
    p.add_argument("--snapshot-every", type=float, default=60, help="seconds per rolling snapshot of every quote; 0 = off")
    p.add_argument("--burst", type=float, default=1, help="tick-rate multiplier during the burst")
    p.add_argument("--burst-at", type=float, default=0, help="seconds after start; 0 = no burst")
    p.add_argument("--burst-for", type=float, default=10)
    asyncio.run(main(p.parse_args()))
