"""CRED's backends, as the lane sees them: every payment status change and balance update for every member,
written once to the payment status lane.

Each event is keyed by member, with a per-member sequence number and the resulting state. The flows are the ones
CRED names for Propeller (bill payments, UPI transfers, reward redemptions, cashback credits, CRED coin balance
updates, CRED Win prize confirmations, failed transactions with clear reasons); the status names are illustrative:
  * UPI payment: PENDING, then SUCCESS, or FAILED with a reason;
  * credit card bill payment: PAYMENT_RECEIVED, PAYOUT_INITIATED, then BILL_PAID (and the CRED coins it earned),
    or PAYOUT_FAILED and REFUNDED;
  * CASHBACK_CREDITED, COINS_BALANCE (the resulting balance), REWARD_REDEEMED, WIN_PRIZE_CONFIRMED.

Writers are on their own connections, each owning some partitions, and write every --tick-ms; an event created at
some point in the last tick waits for that write, and the wait counts in the latency the Propeller nodes measure.
--burst multiplies the rate for --burst-for seconds (a payment peak; CRED has not published its peak shape).
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS, PARTITIONS, STREAM, Window, connect, ensure_topics, member, msg, part_of

WRITERS = PARTITIONS[EVENTS]          # one writer connection per partition
REASONS = ("bank server not responding", "incorrect UPI PIN", "daily UPI limit reached",
           "insufficient balance", "payee bank declined")
BANKS = ("HDFC Bank", "ICICI Bank", "SBI Card", "Axis Bank", "Kotak", "AU Bank")


async def main(a):
    admin = await connect(a.url)
    await ensure_topics(admin, EVENTS)
    P = PARTITIONS[EVENTS]
    writers = [await connect(a.url) for _ in range(WRITERS)]   # partition p is written by writers[p % WRITERS]
    keys = [member(i) for i in range(a.members)]
    rng = random.Random(11)
    seq = defaultdict(int)
    coins = defaultdict(lambda: rng.randrange(2_000, 400_000))
    open_ = defaultdict(dict)             # member -> payment ref -> payment (in flight only)
    n_refs = [0]
    acks, st = Window(), defaultdict(int)

    def ev(k, ref, flow, status, waited_ns, amount=0.0, reason="", bank=""):
        seq[k] += 1
        st[status] += 1
        return k, SendMessage(msg(k=k, seq=seq[k], ref=ref, flow=flow, status=status, amount=round(amount, 2),
                                  reason=reason, bank=bank, coins=coins[k], ns=time.time_ns() - waited_ns))

    def new_ref(prefix):
        n_refs[0] += 1
        return f"{prefix}{6110000000 + n_refs[0]}"

    due_q = []                            # (due time, member, ref): the next status of an in-flight payment

    def later(k, ref, lo, hi):
        heapq.heappush(due_q, (time.monotonic() + rng.uniform(lo, hi), k, ref))

    def advance(k, ref, waited_ns):
        # The next status of an in-flight payment, when it is due.
        p = open_[k].get(ref)
        if p is None:
            return []
        if p["flow"] == "upi":
            del open_[k][ref]
            if rng.random() < a.fail:
                return [ev(k, ref, "upi", "FAILED", waited_ns, p["amt"], rng.choice(REASONS))]
            return [ev(k, ref, "upi", "SUCCESS", waited_ns, p["amt"])]
        if p["step"] == 0:
            p["step"] = 1
            later(k, ref, 1.0, 6.0)
            return [ev(k, ref, "bill", "PAYOUT_INITIATED", waited_ns, p["amt"], bank=p["bank"])]
        del open_[k][ref]
        if rng.random() < a.fail / 2:
            return [ev(k, ref, "bill", "PAYOUT_FAILED", waited_ns, p["amt"], "card issuer rejected the payout",
                       p["bank"]),
                    ev(k, ref, "bill", "REFUNDED", waited_ns, p["amt"], bank=p["bank"])]
        coins[k] += int(p["amt"])            # one coin per rupee paid (illustrative)
        return [ev(k, ref, "bill", "BILL_PAID", waited_ns, p["amt"], bank=p["bank"]),
                ev(k, ref, "coins", "COINS_BALANCE", waited_ns)]

    def one_event(waited_ns):
        # A new action. A few members pay and redeem far more often than the long tail.
        k = keys[int(len(keys) * rng.random() ** 2)]
        r = rng.random()
        if r < 0.62:
            ref = new_ref("upi_")
            open_[k][ref] = {"flow": "upi", "amt": rng.choice((49, 120, 260, 540, 1499, 4200)) * rng.uniform(.8, 1.2)}
            later(k, ref, 0.3, 4.0)          # the bank's response: most in a second or two, some slower
            return [ev(k, ref, "upi", "PENDING", waited_ns, open_[k][ref]["amt"])]
        if r < 0.80:
            ref = new_ref("ccbp_")
            p = open_[k][ref] = {"flow": "bill", "step": 0, "bank": rng.choice(BANKS),
                                 "amt": rng.choice((3_400, 12_800, 27_500, 61_000, 1_48_000)) * rng.uniform(.8, 1.2)}
            later(k, ref, 0.5, 3.0)
            return [ev(k, ref, "bill", "PAYMENT_RECEIVED", waited_ns, p["amt"], bank=p["bank"])]
        if r < 0.88:
            return [ev(k, new_ref("cb_"), "cashback", "CASHBACK_CREDITED", waited_ns, rng.choice((5, 10, 25, 50, 100)))]
        if r < 0.97:
            coins[k] = max(0, coins[k] - rng.choice((500, 1_000, 2_500, 5_000)))
            return [ev(k, new_ref("rr_"), "reward", "REWARD_REDEEMED", waited_ns),
                    ev(k, "", "coins", "COINS_BALANCE", waited_ns)]
        return [ev(k, new_ref("win_"), "win", "WIN_PRIZE_CONFIRMED", waited_ns, rng.choice((10, 100, 1_000)))]

    async def send(items):
        out = defaultdict(list)              # one batch per partition: a member's events stay in order
        for k, m in items:
            out[part_of(k, P)].append(m)
        s0 = time.perf_counter()
        await asyncio.gather(*(writers[p % WRITERS].send_messages(STREAM, EVENTS, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))
        acks.add((time.perf_counter() - s0) * 1e3)

    async def run():
        gc.freeze()                          # keep start-up tables out of the collector's way
        t0 = due = time.monotonic()
        owed = 0.0
        while True:
            el = time.monotonic() - t0
            x = a.burst if a.burst_at and a.burst_at <= el < a.burst_at + a.burst_for else 1
            owed += a.rate * x * a.tick_ms / 1000
            items = []
            now = time.monotonic()
            while due_q and due_q[0][0] <= now:   # in-flight payments whose next status is due
                _, k, ref = heapq.heappop(due_q)
                e = advance(k, ref, int(rng.random() * a.tick_ms * 1e6))
                items += e
                owed -= len(e)
            while owed >= 1:                 # then new actions, so the total stays at the rate
                e = one_event(int(rng.random() * a.tick_ms * 1e6))
                items += e
                owed -= len(e)
            st["late_ms"] = max(st["late_ms"], 1000 * (time.monotonic() - due))
            if items:
                await send(items)
                st["events"] += len(items)
            st["burst"] = int(x > 1)
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def show():
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[payments] {st['events']:>6,} events/s{' (peak burst %gx)' % a.burst if st['burst'] else ''}  "
                  f"UPI PENDING {st['PENDING']:,} SUCCESS {st['SUCCESS']:,} FAILED {st['FAILED']:,}  "
                  f"bills {st['PAYMENT_RECEIVED']:,} paid {st['BILL_PAID']:,} refunded {st['REFUNDED']:,}  "
                  f"cashback {st['CASHBACK_CREDITED']:,}  coins {st['COINS_BALANCE']:,}  in flight {len(due_q):,}  "
                  f"write ack p50 {r['p50']:.1f}ms  p99 {r['p99']:.1f}ms  tick late max {st['late_ms']:.0f}ms",
                  flush=True)
            st.clear()

    print(f"[payments] {a.members:,} members with the app open, {a.rate:,.0f} events/s, {P} partitions keyed by "
          f"member", flush=True)
    await asyncio.gather(run(), show())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--members", type=int, default=500_000, help="members with the app open")
    p.add_argument("--rate", type=float, default=5_000, help="status changes and balance updates per second")
    p.add_argument("--fail", type=float, default=0.06, help="share of UPI payments that fail")
    p.add_argument("--tick-ms", type=float, default=2, help="the writers write once per this many ms")
    p.add_argument("--burst", type=float, default=1, help="rate multiplier at the peak")
    p.add_argument("--burst-at", type=float, default=0, help="seconds after start; 0 = no burst")
    p.add_argument("--burst-for", type=float, default=15)
    asyncio.run(main(p.parse_args()))
