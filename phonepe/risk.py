"""The risk check's aggregate store: payer velocity kept current from the lane, and every check verified against it.

Two readers, each on its own connection and consumer group:
  * payer-velocity (the lane): applies each payment's resulting counts to the payer's entry,
    ignoring any older or repeated sequence. Payment-to-store latency is timed here.
  * risk-checks: plays the risk check on a payer's next payment. It reads the payer's counts
    and asks: did the store already count the payer's previous payment when this one started?
    It also applies an example velocity rule (5 or more payments in 60 s).

Prints payment-to-store p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the
business number: the share of checks on repeat payers that did not count the payer's last payment.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import CHECKS, DEFAULT_URL, RULE_COUNT_60S, STREAM, VELOCITY, Window, ensure_topics, load


async def main(a):
    async def own_client():                 # one connection per reader, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), VELOCITY, CHECKS)

    async def group(topic):
        return await (await own_client()).consumer_group(
            f"{a.name}-{topic}-{os.getpid()}", STREAM, topic, polling_strategy=PollingStrategy.First(), batch_length=5000,
            auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))

    velocity, checks = await group(VELOCITY), await group(CHECKS)
    store = {}                              # payer -> (seq, when applied, payments in 60 s, payees in 10 min)
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays payments made before it joined
    gc.freeze()

    async def on_velocity(m):
        v = load(m.payload())
        p = v["payer"]
        if v["seq"] <= store.get(p, (0,))[0]:
            stats["dropped"] += 1           # older or repeated: the newest counts win
            return
        now = time.time_ns()
        store[p] = (v["seq"], now, v["cnt_60s"], v["payees_10m"])
        age = (now - v["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest payment seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_check(m):
        c = load(m.payload())
        if not caught_up[0] or time.time_ns() - c["ns"] > 5e9:
            return                          # replayed history, not a check happening now
        seq, applied, cnt_60s, _ = store.get(c["payer"], (0, 0, 0, 0))
        stats["checked"] += 1
        stats["burst"] += c["burst"]
        # The check started at c["ns"]. Had the store counted the payer's previous payment by then?
        # (Both clocks are wall clocks; across machines, keep them NTP-synced.)
        if seq < c["needs"] or (seq == c["needs"] and applied > c["ns"]):
            stats["missed"] += 1
            stats["missed_burst"] += c["burst"]
        elif cnt_60s + 1 >= RULE_COUNT_60S:
            stats["rule"] += 1              # this payment makes 5 or more in 60 s: step up or hold

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of payments arrived inside the budget; partitions interleave,
                # so one fresh payment alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier payments this second", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            missed = 100 * stats["missed"] / stats["checked"] if stats["checked"] else 0
            print(f"[{a.name}] {r['n']:>7,} velocity updates/s  payment-to-store p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  payers in store {len(store):,}", flush=True)
            print(f"[check] repeat-payer checks {stats['checked']:,} (in bursts {stats['burst']:,})  "
                  f"missing the previous payment {missed:.3f}% (in bursts {stats['missed_burst']:,})  "
                  f"velocity rule hits {stats['rule']:,}", flush=True)
            stats.clear()

    stop = asyncio.Event()
    await asyncio.gather(velocity.consume_messages(on_velocity, stop), checks.consume_messages(on_check, stop), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="risk")
    p.add_argument("--budget-ms", type=int, default=50, help="payment-to-store p99 budget for the lane")
    asyncio.run(main(p.parse_args()))
