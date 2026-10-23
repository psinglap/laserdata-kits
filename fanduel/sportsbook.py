"""The sportsbook at site nj-1: every account and wallet event it accepts, written once to the state's wallet lane.

Stands in for the account and wallet service at the site taking bets in one state. Customers deposit, place bets,
have bets settled as games finish, cash out and withdraw; every event carries the account's resulting balance and
open bets. A few accounts are busy and most are occasional, as on any sportsbook.

One writer per partition, each with its own connection, writing once per --tick-ms with the events accepted since
the last write, so every account's events stay in order. --rate is the event rate on a normal evening; from
--surge-at for --surge-s seconds it runs --surge times higher, the kickoff shape of a big game (FanDuel scales
capacity 5-10x for events like the Super Bowl). Stops after --duration seconds and prints what it accepted.
"""
import argparse
import asyncio
import gc
import itertools
import random
import time

from apache_iggy import SendMessage

from common import LANE, PARTITIONS, PRIMARY, PRIMARY_URL, STREAM, Window, account_ids, connect, ensure_topic, pack, \
    partition_of


class Account:
    __slots__ = ("id", "seq", "bal", "open")

    def __init__(self, aid: str, rng):
        self.id, self.seq, self.bal, self.open = aid, 0, rng.randrange(2_000, 50_000), []   # cents

    def next(self, rng, n: int, waited_ns: int) -> bytes:
        # One event on this account, applied here first; the message carries the result.
        self.seq += 1
        r = rng.random()
        if self.open and r < 0.45:
            bet, stake, odds = self.open.pop(rng.randrange(len(self.open)))
            if rng.random() < 0.1:
                kind, amt = "cash.out", int(stake * rng.uniform(0.3, 1.6))
            else:
                kind, amt = "bet.settled", (int(stake * odds) if rng.random() < 0.45 else 0)
            self.bal += amt
        elif self.bal < 500 or r > 0.97:
            kind, bet, amt = "deposit", "", rng.choice([2_000, 5_000, 10_000, 25_000])
            self.bal += amt
        elif r > 0.95 and self.bal > 5_000:
            kind, bet, amt = "withdrawal", "", self.bal // 2
            self.bal -= amt
        else:
            kind, bet = "bet.placed", f"b{n:010d}"
            amt = max(100, min(self.bal, int(self.bal * rng.uniform(0.02, 0.3))))
            self.bal -= amt
            self.open.append((bet, amt, rng.choice([1.5, 1.91, 2.2, 3.0, 4.5, 11.0])))
        return pack(self.id, self.seq, kind, bet, amt, self.bal, len(self.open), PRIMARY, waited_ns=waited_ns)


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, LANE, PARTITIONS)
    rng = random.Random(2009)
    accts = [Account(x, rng) for x in account_ids(a.accounts)]
    by_part = [[x for x in accts if partition_of(x.id) == p] for p in range(PARTITIONS)]
    # A few accounts are busy, most bet now and then: activity weight falls off with rank.
    cum = [list(itertools.accumulate(1.0 / (i + 1) ** 0.8 for i in range(len(m)))) for m in by_part]
    acks, sent, bets = Window(), [0], itertools.count(1)
    ready, started, t0 = [0], asyncio.Event(), [0.0]

    def rate_now() -> float:
        el = time.monotonic() - t0[0]
        return a.rate * (a.surge if a.surge_at <= el < a.surge_at + a.surge_s else 1.0)

    async def writer(pid):
        c = await connect(a.url)
        mine, prng, carry = by_part[pid], random.Random(pid), 0.0
        ready[0] += 1
        await started.wait()
        due = time.monotonic()
        while time.monotonic() - t0[0] < a.duration:
            carry += rate_now() / PARTITIONS * a.tick_ms / 1000
            k, carry = int(carry), carry - int(carry)
            if k and mine:
                now = time.monotonic()
                # Events accepted since the last write wait for this write; that wait counts as latency.
                waited = int(max(0.0, now - due) * 1e9)
                picks = prng.choices(mine, cum_weights=cum[pid], k=k)
                batch = [SendMessage(x.next(prng, next(bets), waited)) for x in picks]
                s0 = time.perf_counter()
                await c.send_messages(STREAM, LANE, pid, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
                sent[0] += len(batch)
            due += a.tick_ms / 1000                             # from the due time, so a slow tick doesn't drift
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def go():
        while ready[0] < PARTITIONS:
            await asyncio.sleep(0.05)
        print(f"[sportsbook {PRIMARY}] {len(accts):,} accounts across {PARTITIONS} partitions; {a.rate:g} events/s, "
              f"{a.surge:g}x from {a.surge_at:g}s for {a.surge_s:g}s", flush=True)
        gc.freeze()                                             # keep startup objects out of the collector's way
        t0[0] = time.monotonic()
        started.set()

    async def report():
        await started.wait()
        total = 0
        while time.monotonic() - t0[0] < a.duration + 0.5:
            await asyncio.sleep(1)
            r = acks.flush()
            total += sent[0]
            el = time.monotonic() - t0[0]
            tag = "  kickoff surge" if a.surge_at <= el - 1 < a.surge_at + a.surge_s else ""
            print(f"[sportsbook {PRIMARY}] {sent[0]:>5,} events/s accepted  write ack p50 {r['p50']:.1f}ms  "
                  f"p99 {r['p99']:.1f}ms{tag}", flush=True)
            sent[0] = 0
        total += sent[0]
        print(f"[sportsbook {PRIMARY}] stopped: {total:,} events accepted on {PRIMARY}", flush=True)

    await asyncio.gather(go(), report(), *(writer(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=PRIMARY_URL, help="the broker at the site taking bets (nj-1)")
    p.add_argument("--accounts", type=int, default=200_000, help="active accounts in the state (illustrative)")
    p.add_argument("--rate", type=float, default=600, help="account and wallet events a second on a normal evening")
    p.add_argument("--surge", type=float, default=5, help="kickoff multiplier (FanDuel scales 5-10x for big events)")
    p.add_argument("--surge-at", type=float, default=30)
    p.add_argument("--surge-s", type=float, default=20)
    p.add_argument("--duration", type=float, default=3600)
    p.add_argument("--tick-ms", type=float, default=2, help="each writer writes once per this many ms")
    asyncio.run(main(p.parse_args()))
