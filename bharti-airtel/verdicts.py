"""The verdict writer: every new verdict on a domain from the Fraud Detection Solution, written once to the lane.

Stands in for the places Airtel's verdicts come from: the SMS link scan (every SMS checked against the
database of malicious links), partner API scores on domains not yet in the database, threat feeds, and
customer reports. For each domain it keeps a verdict (block or clean) and a composite fraud score, and
writes a message whenever a domain is scored. Most messages are re-scores with the same verdict. A few each
second are flips, which are what every enforcement point must see first:
  * clean -> block: a domain first scored clean is now known to be malicious (say the SMS link scan finds it
    in a burst of SMS from flagged senders). Until a resolver has the block, clicks on it still resolve.
  * block -> clean: a block lifted (a false positive cleared). Until a resolver has it, legitimate lookups
    still land on the warning page.

Smishing campaign: from --campaign-at for --campaign-for seconds, --campaign-x times the usual flips, all to
block and all from the SMS link scan, as when one SMS blast carries a fresh set of links.

One writer per partition, each with its own connection, writes every --tick-ms, so each domain's verdicts
stay in order. On start it writes one snapshot message per domain (sequence 1). After that it re-writes every
domain's current verdict once per --snapshot-every seconds, spread over the period, with the domain's current
sequence (live readers skip it as a repeat), so a resolver that starts late only replays the last minute or
so of the log to hold every verdict.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import BLOCK, CLEAN, DEFAULT_URL, PARTITIONS, STREAM, VERDICTS, Window, connect, domains, ensure_topic, msg, part


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, VERDICTS)
    n_parts = PARTITIONS[VERDICTS]
    keys = domains(a.domains)
    by_part = [[k for k in keys if part(k) == p] for p in range(n_parts)]
    rng = random.Random(7)
    state = {}                                   # domain -> [verdict, score, seq]
    for k in keys:
        b = rng.random() < a.blocked_share
        state[k] = [BLOCK if b else CLEAN, rng.uniform(0.8, 1.0) if b else rng.uniform(0.0, 0.5), 0]
    acks, sent, flips, t0 = Window(), [0], [0, 0], time.monotonic()
    snapped = [0]
    gc.freeze()                                  # keep start-up tables out of the collector's way

    def write(k, src, waited_ns=0, repeat=False):
        st = state[k]
        if not repeat:
            st[2] += 1
        return SendMessage(msg(k, st[2], st[0], st[1], src, waited_ns))

    def campaign():
        el = time.monotonic() - t0
        return a.campaign_for > 0 and a.campaign_at <= el < a.campaign_at + a.campaign_for

    def rescore(k, waited):
        st = state[k]
        lo, hi = (0.8, 1.0) if st[0] == BLOCK else (0.0, 0.5)
        st[1] = min(hi, max(lo, st[1] + rng.uniform(-0.05, 0.05)))
        return write(k, rng.choice(("api", "api", "feed")), waited)

    def flip(mine, to, src, waited):
        for _ in range(50):                      # a domain currently on the other verdict
            k = mine[rng.randrange(len(mine))]
            if state[k][0] != to:
                break
        else:
            return None
        st = state[k]
        st[0], st[1] = to, (rng.uniform(0.85, 0.99) if to == BLOCK else rng.uniform(0.05, 0.3))
        flips[0 if to == BLOCK else 1] += 1
        return write(k, src, waited)

    async def writer(p):
        c = await connect(a.url)
        mine = by_part[p]
        for i in range(0, len(mine), 5000):                          # snapshot: every domain, sequence 1
            await c.send_messages(STREAM, VERDICTS, p, [write(k, "feed") for k in mine[i:i + 5000]])
        snapped[0] += 1
        await started.wait()
        share = len(mine) / len(keys)
        due, carry, fcarry, ccarry, cursor, snap = time.monotonic(), 0.0, 0.0, 0.0, 0, 0.0
        snap_per_write = len(mine) * a.tick_ms / (a.snapshot_every * 1000) if a.snapshot_every else 0
        tick = a.tick_ms / 1000
        while True:
            carry += a.rate * tick * share
            fcarry += a.flips * tick * share
            if campaign():                                           # a smishing blast: extra blocks from the SMS scan
                ccarry += a.flips * tick * share * (a.campaign_x - 1)
            n, carry = int(carry), carry - int(carry)
            f, fcarry = int(fcarry), fcarry - int(fcarry)
            cf, ccarry = int(ccarry), ccarry - int(ccarry)
            batch = []
            snap += snap_per_write                                   # rolling snapshot: a slice of domains,
            if n or f or cf or snap >= 10 * snap_per_write:          # riding on writes that carry live verdicts
                for _ in range(int(snap)):
                    batch.append(write(mine[cursor], "feed", repeat=True))
                    cursor = (cursor + 1) % len(mine)
                snap -= int(snap)
            # A verdict is set at some point in the last --tick-ms and waits for this write; that wait counts.
            w = lambda: int(rng.random() * a.tick_ms * 1e6)
            for _ in range(n):                                       # re-scores; some domains looked up far more
                batch.append(rescore(mine[int(len(mine) * rng.random() ** 2)], w()))
            for _ in range(f):                                       # flips: mostly to block, some blocks lifted
                to = CLEAN if rng.random() < a.clear_share else BLOCK
                m = flip(mine, to, rng.choice(("sms", "api", "feed", "rpt")) if to == BLOCK else "rpt", w())
                if m:
                    batch.append(m)
            for _ in range(cf):
                m = flip(mine, BLOCK, "sms", w())
                if m:
                    batch.append(m)
            sent[0] += n + f + cf
            if batch:
                s0 = time.perf_counter()
                await c.send_messages(STREAM, VERDICTS, p, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
            due += tick                                              # from the due time, so a slow write doesn't drift
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        await started.wait()
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            tag = f"  smishing campaign x{a.campaign_x:g}" if campaign() else ""
            print(f"[verdicts] {sent[0]:>7,} verdicts/s  to block {flips[0]:,}  lifted {flips[1]:,}  {r['n']:,} writes  "
                  f"write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms{tag}", flush=True)
            sent[0] = flips[0] = flips[1] = 0

    started = asyncio.Event()

    async def go():
        nonlocal t0
        while snapped[0] < n_parts:
            await asyncio.sleep(0.05)
        blocked = sum(1 for v in state.values() if v[0] == BLOCK)
        print(f"[verdicts] snapshot: {len(keys):,} domains, {blocked:,} blocked", flush=True)
        t0 = time.monotonic()
        started.set()

    await asyncio.gather(go(), report(), *(writer(p) for p in range(n_parts)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--domains", type=int, default=500_000, help="domains with a verdict (the database holds 187K+ blocked ones)")
    p.add_argument("--blocked-share", type=float, default=0.4, help="share of those domains blocked at start")
    p.add_argument("--rate", type=int, default=3000, help="verdicts per second: re-scores of domains being looked up")
    p.add_argument("--flips", type=float, default=5, help="per second: a verdict flips (clean to block, or a block lifted)")
    p.add_argument("--clear-share", type=float, default=0.1, help="share of flips that lift a block")
    p.add_argument("--campaign-at", type=float, default=60, help="a smishing campaign starts this many seconds in")
    p.add_argument("--campaign-for", type=float, default=30, help="seconds; 0 = no campaign")
    p.add_argument("--campaign-x", type=float, default=10, help="blocks during the campaign, times the usual flips")
    p.add_argument("--tick-ms", type=float, default=2, help="each writer writes once per this many ms")
    p.add_argument("--snapshot-every", type=float, default=60, help="seconds per rolling snapshot of every verdict; 0 = off")
    asyncio.run(main(p.parse_args()))
