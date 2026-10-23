"""The payments platform side: the token vault (the source of truth for every customer's payment token), the
scheduled token jobs that change it, and the check of every instalment charge against it.

Each token change goes to the payment-token-changes topic, keyed by customer, with a per-customer sequence number
and the resulting token (token ID, last four, expiry, status), never only a delta:
  * card_updater: the network's account updater returns a new expiry or a reissued card (more of these during a
    scheduled updater run, --run);
  * card_replaced: the customer adds a new card in the app and makes it the default;
  * token_revoked: the card is removed or closed; the customer has no usable card until they add one.

Before a change is signed it goes through the card number guard: any 13-19 digit run that passes the Luhn check
(in a free-text field such as an updater response note) is masked to its last four. Then the record is signed
with HMAC-SHA256, so a subscriber can prove it came from the payments platform and was not changed on the way.

Subscribers (instalments.py) send each instalment charge to instalment-charges with the token version they used.
This script checks each one against the token that was current at the moment the charge was made, and prints
every second how many went out on a superseded token (they would decline), and how many a subscriber fed --lag
seconds behind would have sent on one.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, Partitioning, PollingStrategy, SendMessage

from common import (CHARGES, DEFAULT_URL, PARTITIONS, STREAM, TOKENS, connect, customer, ensure_topics, load,
                    mask_record, msg, part_of, sign)

# Card numbers from the networks' public test ranges; they only ever appear in the demo's updater notes.
TEST_PANS = ("4111111111111111", "5555555555554444", "378282246310005", "6011111111111117")


async def main(a):
    admin = await connect(a.url)
    await ensure_topics(admin, TOKENS, CHARGES)
    lane = await connect(a.url)                                      # one connection per writer and reader
    charges = await (await connect(a.url)).consumer_group(
        "token-vault-check", STREAM, CHARGES, polling_strategy=PollingStrategy.Next(), batch_length=1000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))

    keys = [customer(i) for i in range(a.customers)]
    rng = random.Random(11)
    seq = defaultdict(int)
    token = {}                               # customer -> (token id, last four, expiry, status)
    history = defaultdict(lambda: deque(maxlen=8))   # customer -> recent (seq, created ns): which token was current when
    st = defaultdict(int)
    n_tok = [0]

    def new_token(k, status="active"):
        n_tok[0] += 1
        return (f"tok_{n_tok[0]:010d}", f"{rng.randrange(10_000):04d}",
                f"{rng.randint(1, 12):02d}/{rng.randint(27, 31)}", status)

    def change(k, kind, note=None):
        seq[k] += 1
        tid, last4, exp, status = token[k]
        d = dict(k=k, seq=seq[k], kind=kind, token=tid, last4=last4, exp=exp, status=status, ns=time.time_ns())
        if note is not None:
            d["note"] = note
        st["masked"] += mask_record(d)                      # the guard runs before signing and before the send
        history[k].append((seq[k], d["ns"]))
        return k, SendMessage(sign(msg(**d)))

    async def send(items):
        # One batch per partition: a customer's changes stay in order in one partition.
        out = defaultdict(list)
        for k, m in items:
            out[part_of(k)].append(m)
        await asyncio.gather(*(lane.send_messages(STREAM, TOKENS, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))

    def one_change(k, updater_run):
        r = rng.random()
        tid, last4, exp, status = token[k]
        if status == "revoked" or (not updater_run and r < 0.30):
            token[k] = new_token(k)
            st["card_replaced"] += 1
            return change(k, "card_replaced")
        if updater_run or r < 0.92:
            reissued = rng.random() < 0.25                   # a new card number, or only a new expiry
            fresh = new_token(k)
            token[k] = fresh if reissued else (fresh[0], last4, fresh[2], "active")
            st["card_updater"] += 1
            note = None
            if a.pan_every and rng.randrange(a.pan_every) == 0:
                note = f"updater response: replaced {rng.choice(TEST_PANS)} reason=REISSUE"   # a raw number in a note
            return change(k, "card_updater", note)
        token[k] = (tid, last4, exp, "revoked")
        st["token_revoked"] += 1
        return change(k, "token_revoked")

    async def snapshot():
        # Every customer's current token, like a CDC snapshot (seq 1, not timed as a live change).
        for k in keys:
            token[k] = new_token(k)
        for i in range(0, len(keys), 5000):
            await send([change(k, "snapshot") for k in keys[i:i + 5000]])
        print(f"[tokens] snapshot: {len(keys):,} customers' payment tokens", flush=True)

    async def run():
        await snapshot()
        gc.freeze()                          # keep start-up tables out of the collector's way
        t0 = due = time.monotonic()
        owed = 0.0
        while True:
            t = time.monotonic() - t0
            updater_run = bool(a.run_at and a.run_at <= t < a.run_at + a.run_for)
            owed += a.rate * (a.run if updater_run else 1) / 100
            n = int(owed)
            owed -= n
            items = [one_change(rng.choice(keys), updater_run and rng.random() < 1 - 1 / a.run) for _ in range(n)]
            t_send = time.monotonic()
            if items:
                await send(items)
            st["send_ms"] = max(st["send_ms"], 1000 * (time.monotonic() - t_send))
            st["late_ms"] = max(st["late_ms"], 1000 * (t_send - due))     # how late this tick started
            st["changes"] += n
            st["run"] = int(updater_run)
            due += 0.01                                                    # from the due time, not from now
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    def current_at(k, ns):
        # The token version that was current when the charge was made: the newest change created before it.
        h = history.get(k)
        if not h:
            return 1
        best = h[0][0] - 1
        for s, created in h:
            if created <= ns:
                best = s
        return best

    async def on_charge(m):
        c = load(m.payload())
        need = current_at(c["k"], c["ns"])   # both picks are judged against this same state
        st["charges"] += 1
        if c["seq"] < need:
            st["stale"] += 1                 # sent on a token the vault had already superseded: it would decline
        if c["lseq"] < need:
            st["lag_stale"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            print(f"[tokens] {st['changes']:>6,} changes/s ({'updater run' if st['run'] else 'steady'})  "
                  f"(card_updater {st['card_updater']:,} card_replaced {st['card_replaced']:,} "
                  f"token_revoked {st['token_revoked']:,})  card numbers masked {st['masked']:,}  "
                  f"charges checked {st['charges']:,}  on a superseded token {st['stale']:,}  "
                  f"a subscriber {a.lag:g}s behind would send {st['lag_stale']:,}  "
                  f"tick late max {st['late_ms']:.0f}ms  send max {st['send_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(run(), charges.consume_messages(on_charge, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--customers", type=int, default=500_000, help="customers with a payment token on file")
    p.add_argument("--rate", type=float, default=500, help="token changes per second outside an updater run")
    p.add_argument("--run", type=float, default=1.0, help="change-rate multiplier during a scheduled updater run")
    p.add_argument("--run-at", type=float, default=0, help="seconds after start")
    p.add_argument("--run-for", type=float, default=60)
    p.add_argument("--pan-every", type=int, default=2000, help="one updater note in N carries a raw card number (0 = none)")
    p.add_argument("--lag", type=float, default=5.0, help="the comparison subscriber's lag, as set on instalments.py (label only)")
    asyncio.run(main(p.parse_args()))
