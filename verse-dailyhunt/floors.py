"""The floor service: every new floor for a Dailyhunt placement, written once to the lane.

Stands in for the part of NexVerse.ai's yield engine that sets floors from what it sees in auctions
(bids, clearing prices, fill, 204 no-bids). For each placement (language x ad slot) it keeps a floor in
rupees CPM and writes a message whenever that floor changes. Most changes are small moves; a few each
second are steps, where the floor jumps up because demand rose or down because bids stopped clearing it.
Steps are what bid nodes must see first: until a node has the new floor it sells below it (a raise) or
lets impressions go unfilled under a floor that is now too high (a drop).

Breaking news: from --spike-at for --spike-for seconds, one language (--spike-lang) sees --spike-x times
its usual floor changes, and its steps are mostly raises, as when an election result or a cricket finish
sends readers and advertisers to the same stories at once.

One writer per partition, each with its own connection, writes every --tick-ms, so each placement's
floors stay in order. On start it writes one snapshot message per placement (sequence 1). After that it
re-writes every placement's current floor once per --snapshot-every seconds, spread over the period, with
the placement's current sequence (live readers skip it as a repeat), so a bid node that starts or is added
by the autoscaler only replays the last minute or so of the log to have every floor.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, FLOORS, LANGS, PARTITIONS, STREAM, Window, connect, ensure_topic, msg, part, placements


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, FLOORS)
    n_parts = PARTITIONS[FLOORS]
    keys = placements(a.placements_per_lang, a.langs)
    by_part = [[k for k in keys if part(k) == p] for p in range(n_parts)]
    hot = [[k for k in mine if k.split("|", 1)[0] == a.spike_lang] for mine in by_part]
    rng = random.Random(11)
    state = {k: [rng.uniform(a.floor_min, a.floor_max), 0] for k in keys}   # placement -> [floor Rs CPM, seq]
    acks, sent, steps, t0 = Window(), [0], [0], time.monotonic()
    snapped = [0]
    gc.freeze()                                      # keep start-up tables out of the collector's way

    def change(k, step=0, waited_ns=0, repeat=False):
        st = state[k]
        if not repeat:
            st[1] += 1
        return SendMessage(msg(k, st[1], st[0], step, waited_ns))

    def spiking():
        el = time.monotonic() - t0
        return a.spike_for > 0 and a.spike_at <= el < a.spike_at + a.spike_for

    def move(k, waited):
        st = state[k]
        st[0] = min(a.floor_max * 3, max(a.floor_min / 2, st[0] * rng.uniform(0.96, 1.04)))
        return change(k, waited_ns=waited)

    def jump(k, up, waited):
        st = state[k]
        st[0] = min(a.floor_max * 3, st[0] * rng.uniform(1.3, 1.7)) if up else max(a.floor_min / 2, st[0] * rng.uniform(0.6, 0.8))
        steps[0] += 1
        return change(k, step=1 if up else -1, waited_ns=waited)

    async def writer(p):
        c = await connect(a.url)
        mine, mine_hot = by_part[p], hot[p]
        for i in range(0, len(mine), 5000):                          # snapshot: every placement, sequence 1
            await c.send_messages(STREAM, FLOORS, p, [change(k) for k in mine[i:i + 5000]])
        snapped[0] += 1
        await started.wait()
        share, hot_share = len(mine) / len(keys), len(mine_hot) / len(keys)
        due, carry, fcarry, hcarry, hfcarry, cursor, snap = time.monotonic(), 0.0, 0.0, 0.0, 0.0, 0, 0.0
        snap_per_write = len(mine) * a.tick_ms / (a.snapshot_every * 1000) if a.snapshot_every else 0
        tick = a.tick_ms / 1000
        while True:
            carry += a.rate * tick * share
            fcarry += a.steps * tick * share
            if spiking() and mine_hot:                               # breaking news on one language
                hcarry += a.rate * tick * hot_share * (a.spike_x - 1)
                hfcarry += a.steps * tick * hot_share * (a.spike_x - 1)
            n, carry = int(carry), carry - int(carry)
            f, fcarry = int(fcarry), fcarry - int(fcarry)
            hn, hcarry = int(hcarry), hcarry - int(hcarry)
            hf, hfcarry = int(hfcarry), hfcarry - int(hfcarry)
            batch = []
            snap += snap_per_write                                   # rolling snapshot: a slice of placements,
            if n or f or hn or hf or snap >= 10 * snap_per_write:    # riding on writes that carry live changes
                for _ in range(int(snap)):
                    batch.append(change(mine[cursor], repeat=True))
                    cursor = (cursor + 1) % len(mine)
                snap -= int(snap)
            # A change happens at some point in the last --tick-ms and waits for this write; that wait counts.
            w = lambda: int(rng.random() * a.tick_ms * 1e6)
            for _ in range(n):                                       # small moves; some placements far busier
                batch.append(move(mine[int(len(mine) * rng.random() ** 2)], w()))
            for _ in range(hn):
                batch.append(move(mine_hot[int(len(mine_hot) * rng.random() ** 2)], w()))
            for _ in range(f):                                       # steps, up or down
                batch.append(jump(mine[rng.randrange(len(mine))], rng.random() < 0.5, w()))
            for _ in range(hf):                                      # breaking-news steps: mostly up
                batch.append(jump(mine_hot[rng.randrange(len(mine_hot))], rng.random() < 0.8, w()))
            sent[0] += n + f + hn + hf
            if batch:
                s0 = time.perf_counter()
                await c.send_messages(STREAM, FLOORS, p, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
            due += tick                                              # from the due time, so a slow write doesn't drift
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        await started.wait()
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            tag = f"  breaking news: {a.spike_lang} x{a.spike_x:g}" if spiking() else ""
            print(f"[floors] {sent[0]:>7,} floor changes/s  steps {steps[0]:,}  {r['n']:,} writes  "
                  f"write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms{tag}", flush=True)
            sent[0] = steps[0] = 0

    started = asyncio.Event()

    async def go():
        nonlocal t0
        while snapped[0] < n_parts:
            await asyncio.sleep(0.05)
        print(f"[floors] snapshot: {len(keys):,} placements ({a.langs} languages x {a.placements_per_lang:,} ad slots), "
              f"floors Rs {a.floor_min:g}-{a.floor_max:g} CPM", flush=True)
        t0 = time.monotonic()
        started.set()

    await asyncio.gather(go(), report(), *(writer(p) for p in range(n_parts)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--langs", type=int, default=len(LANGS), help="Dailyhunt languages (up to 15)")
    p.add_argument("--placements-per-lang", type=int, default=2000, help="ad slots per language your floors are keyed on")
    p.add_argument("--floor-min", type=float, default=20, help="lowest starting floor, Rs CPM")
    p.add_argument("--floor-max", type=float, default=180, help="highest starting floor, Rs CPM")
    p.add_argument("--rate", type=int, default=5000, help="floor changes per second, all placements")
    p.add_argument("--steps", type=float, default=100, help="per second: a floor jumps up or down")
    p.add_argument("--spike-lang", default="hi", help="the language breaking news lands on")
    p.add_argument("--spike-at", type=float, default=60, help="breaking news starts this many seconds in")
    p.add_argument("--spike-for", type=float, default=30, help="seconds; 0 = no spike")
    p.add_argument("--spike-x", type=float, default=4, help="that language's floor changes, times its usual")
    p.add_argument("--tick-ms", type=float, default=2, help="each writer writes once per this many ms")
    p.add_argument("--snapshot-every", type=float, default=60, help="seconds per rolling snapshot of every floor; 0 = off")
    asyncio.run(main(p.parse_args()))
