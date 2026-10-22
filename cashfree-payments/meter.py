"""The live usage meter, fed by the usage lane: every payment's usage into the subscription's usage for the day.

Reads the usage topic directly, partition by partition, on --readers connections of its own (no consumer group:
a group reader visits one partition per poll, so with 16 partitions a message waits for its partition's turn).
Each message carries the Kill Bill plugin's usage records for one payment plus the subscription's txnCount and
txnVolume for the day so far, with a per-subscription sequence, so the meter counts each payment once:
  * a message whose sequence it has already applied (a retry, or a replay after a restart) adds nothing;
  * a newer one sets the subscription's day to the totals it carries, so a lost message in between is covered
    by the next one;
  * the meter also sums the usage records themselves and checks them against those totals, to the paisa: a
    subscription whose records and totals disagree (a lost or reordered message, or a broken rule) is counted.
Apache Iggy delivers at least once; counting once comes from this rule, which is why the lane carries totals.

Every --daily-ms the changed subscriptions' day totals go out to usage-daily, as resulting values, for the
merchant's live usage view and plan-limit checks. Kill Bill keeps billing from its own writer (killbill.py).

Prints payment-to-usage p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the billing
numbers: txnCount and txnVolume metered today, subscriptions reconciled, retries absorbed, and what a meter
that adds every message's records would have over-counted.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import DAILY, DEFAULT_URL, PARTITIONS, STREAM, USAGE, Window, amounts, ensure_topics, load, msg, rupees


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), USAGE, DAILY)
    out = await (await own_client()).producer(STREAM, DAILY, topic_partitions_count=PARTITIONS[DAILY])
    lat, stats = Window(), defaultdict(float)
    last = {}                               # subscription -> (sequence, recordDate, txnCount, txnVolume) applied
    recs = {}                               # subscription -> (recordDate, txnCount, txnVolume) summed from records
    bad = set()                             # subscriptions whose records and totals disagreed
    changed = set()
    tot = defaultdict(float)                # since start: absorbed, naive_cnt, naive_vol, cnt, vol
    caught_up = [False]                     # a new reader first replays usage from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_usage(m):
        u = load(m.payload())
        age = (time.time_ns() - u["ns"]) / 1e6
        sub = u["subscriptionExternalKey"]
        day, c, v = amounts(u)
        tot["naive_cnt"] += c               # what a meter that adds every message's records would count
        tot["naive_vol"] += v
        prev = last.get(sub)
        if prev and u["seq"] <= prev[0]:
            tot["absorbed"] += 1            # a retry or a replay: already counted
            return
        if not caught_up[0]:                # still replaying: track the oldest new message seen this second
            stats["replayed"] += 1          # (a retry carries its original send time, so it is left out)
            stats["max_age"] = max(stats["max_age"], age)
        pc, pv = (prev[2], prev[3]) if prev and prev[1] == day else (0, 0)
        tot["cnt"] += u["cnt"] - pc         # the day's totals as carried, so a lost message is covered
        tot["vol"] += u["vol"] - pv
        last[sub] = (u["seq"], day, u["cnt"], u["vol"])
        rd, rc, rv = recs.get(sub, (day, 0, 0))
        if rd != day:
            rc, rv = 0, 0
        rc, rv = rc + c, rv + v
        recs[sub] = (day, rc, rv)
        if (rc, rv) != (u["cnt"], u["vol"]):
            bad.add(sub)                    # records and totals disagree: a message was lost or reordered
        changed.add(sub)
        stats["applied"] += 1
        if caught_up[0]:
            lat.add(age)                    # live usage, from the payment service's send to the day total updated

    async def send_daily():
        async with out:
            while True:
                await asyncio.sleep(a.daily_ms / 1000)
                if not changed:
                    continue
                batch = [SendMessage(msg(sub=s, day=last[s][1], seq=last[s][0], txnCount=last[s][2],
                                         txnVolume=last[s][3])) for s in changed]
                changed.clear()
                for i in range(0, len(batch), 1000):
                    await out.send(batch[i:i + 1000])
                stats["daily"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, USAGE, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_usage(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of usage arrived inside the budget; partitions interleave,
                # so one fresh message alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier usage messages this second",
                      flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} usage msgs/s  payment-to-usage p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"daily totals sent {int(stats['daily']):,}", flush=True)
            over_c, over_v = tot["naive_cnt"] - tot["cnt"], tot["naive_vol"] - tot["vol"]
            print(f"[usage] txnCount {int(tot['cnt']):,}  txnVolume {rupees(tot['vol'])}  "
                  f"subscriptions {len(last):,}  reconciled to the paisa {len(last) - len(bad):,}/{len(last):,}  "
                  f"retries absorbed {int(tot['absorbed']):,}  "
                  f"without dedupe +{int(over_c):,} txns (+{rupees(over_v)}, "
                  f"{100 * over_v / tot['vol'] if tot['vol'] else 0:.2f}%)", flush=True)
            stats.clear()

    parts = list(range((await (await own_client()).get_topic(STREAM, USAGE)).partitions_count))
    await asyncio.gather(send_daily(), report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the usage partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="meter")
    p.add_argument("--budget-ms", type=int, default=100, help="payment-to-usage p99 budget for the lane")
    p.add_argument("--daily-ms", type=int, default=250, help="publish changed day totals this often")
    asyncio.run(main(p.parse_args()))
