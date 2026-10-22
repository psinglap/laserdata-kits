"""The transaction feature store: account spend kept current from the lane, and every authorization check verified against it.

Two readers, each reading every partition of its topic directly on connections of its own (no consumer
group: a group reader visits one partition per poll, so with 16 partitions a record waited for its turn):
  * account-spend (the lane): applies each approval's or reversal's resulting spend to the account's entry,
    ignoring any older or repeated sequence. Authorization-to-store latency is timed here.
  * auth-checks: plays the decision engine on an account's next authorization. It reads the account's
    spend in the category and asks: had the store applied the account's last change when this
    authorization arrived? Then it decides against the add-on card's category limit with the store's
    spend, and compares with the decision on the true spend.

Prints authorization-to-store p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the
business numbers: the share of checks that missed the account's last change, approvals the stale store
would have let through over the category limit, and declines it would have made in error.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import CHECKS, DEFAULT_URL, PARTITIONS, SPEND, STREAM, Window, ensure_topics, load


async def main(a):
    async def own_client():                 # one connection per reader, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SPEND, CHECKS)

    store = {}                              # account -> its last few versions: [(seq, when applied, {category: paise})]
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays changes made before it joined
    gc.freeze()

    async def on_spend(m):
        v = load(m.payload())
        acc = v["account"]
        versions = store.setdefault(acc, [])
        if versions and v["seq"] <= versions[-1][0]:
            stats["dropped"] += 1           # older or repeated: the newest spend wins
            return
        now = time.time_ns()
        versions.append((v["seq"], now, v["spend"]))
        del versions[:-4]                   # a few versions, so a check sees what the store held when it arrived
        stats["reversals"] += v["kind"] == "REVERSED"
        age = (now - v["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest change seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_check(m):
        c = load(m.payload())
        if not caught_up[0] or time.time_ns() - c["ns"] > 5e9:
            return                          # replayed history, not an authorization happening now
        # What the store held when the authorization arrived at c["ns"]: the newest version applied by then.
        # (Both clocks are wall clocks; across machines, keep them NTP-synced.)
        seq, spend = 0, {}
        for s_, applied, sp_ in reversed(store.get(c["account"], ())):
            if applied <= c["ns"]:
                seq, spend = s_, sp_
                break
        stats["checked"] += 1
        stats["run"] += c["run"]
        if seq < c["needs"]:                # the account's last change had not reached the store in time
            stats["missed"] += 1
            stats["missed_run"] += c["run"]
        if not c["limit"]:
            return                          # primary card: no category limit to check
        store_ok = spend.get(c["cat"], 0) + c["amount"] <= c["limit"]
        true_ok = c["true_spend"] + c["amount"] <= c["limit"]
        if not true_ok:
            stats["at_limit"] += 1          # over the category limit, and the store agrees unless counted below
        if store_ok and not true_ok:
            stats["over_limit"] += 1        # the store's spend was behind: approved past the limit
            stats["over_paise"] += c["true_spend"] + c["amount"] - c["limit"]
        elif true_ok and not store_ok:
            stats["wrong_decline"] += 1     # a reversal not yet applied: declined a card that had room

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of changes arrived inside the budget; partitions interleave,
                # so one fresh change alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier changes this second", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            missed = 100 * stats["missed"] / stats["checked"] if stats["checked"] else 0
            print(f"[{a.name}] {r['n']:>7,} spend updates/s  auth-to-store p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  reversals {stats['reversals']:,}  "
                  f"accounts in store {len(store):,}", flush=True)
            print(f"[check] repeat-account checks {stats['checked']:,} (in add-on runs {stats['run']:,})  "
                  f"missing the last change {missed:.3f}% (in runs {stats['missed_run']:,})  "
                  f"declined at the limit {stats['at_limit']:,}  over-limit approvals {stats['over_limit']:,} "
                  f"(Rs {stats['over_paise'] / 100:,.0f} over)  wrong declines {stats['wrong_decline']:,}", flush=True)
            stats.clear()

    async def read(topic, handler, pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from this reader's
        # own position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{topic}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, topic, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        await handler(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def readers(topic, handler):
        parts = list(range(PARTITIONS[topic]))
        return [read(topic, handler, parts[r::a.readers]) for r in range(a.readers)]

    await asyncio.gather(*readers(SPEND, on_spend), *readers(CHECKS, on_check), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="store")
    p.add_argument("--budget-ms", type=int, default=50, help="authorization-to-store p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading each topic's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
