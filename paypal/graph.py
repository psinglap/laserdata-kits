"""The real-time graph's write path and its two-hop check: links kept current from the lane, every payment checked.

Two readers, each reading every partition of its topic directly on connections of its own (no consumer
group: a group reader visits one partition per poll, so with 16 partitions a record waited for its turn):
  * asset-links (the lane): applies each account-to-asset link to the graph (account -> assets and
    asset -> accounts), ignoring any older or repeated sequence. Link-to-graph latency is timed here.
  * payment-checks: plays the risk check on a payment. It runs the two-hop query PayPal's post
    describes (account -> asset -> accounts) for restricted accounts, and asks first: did the graph
    already hold every link from this account's login when the payment started?

Prints link-to-graph p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business
numbers: the share of payment checks that ran without the account's newest links, how many repeat
fraudsters the check linked to restricted accounts, and how many of those checks a write path with
--slow-path-s of freshness would have run before the links landed (judged on each check's own
login-to-payment gap, the same for both paths).
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import CHECKS, DEFAULT_URL, LINKS, PARTITIONS, SLOW_PATH_S, STREAM, Window, ensure_topics, load


async def main(a):
    async def own_client():                 # one connection per reader, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LINKS, CHECKS)

    edges = {}                              # (account, asset) -> (seq, when applied)
    assets_of = defaultdict(set)            # account -> assets
    accounts_on = defaultdict(set)          # asset -> accounts
    restricted = set()
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays links made before it joined
    gc.freeze()

    async def on_link(m):
        e = load(m.payload())
        k = (e["acct"], e["asset"])
        if e["seq"] <= edges.get(k, (0,))[0]:
            return                          # older or repeated: the newest state of the edge wins
        now = time.time_ns()
        edges[k] = (e["seq"], now)
        assets_of[e["acct"]].add(e["asset"])
        accounts_on[e["asset"]].add(e["acct"])
        if e.get("restricted"):
            restricted.add(e["acct"])
        if e.get("snap"):
            return                          # the start-up snapshot is not a live link
        age = (now - e["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest link seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    def two_hop(acct):
        # account -> its assets -> other accounts on them: how many are restricted?
        return sum(len(accounts_on[x] & restricted) for x in assets_of.get(acct, ()))

    async def on_check(m):
        c = load(m.payload())
        if not caught_up[0] or time.time_ns() - c["ns"] > 5e9:
            return                          # replayed history, not a payment happening now
        fraud = c["fraud"]
        stats["checked"] += 1
        stats["fraud"] += fraud
        # The check started at c["ns"]. Did the graph hold every link from the login by then?
        # (Both clocks are wall clocks; across machines, keep them NTP-synced.)
        late = any(edges.get((c["acct"], x), (0, 0))[0] < s or edges[(c["acct"], x)][1] > c["ns"] for x, s in c["links"])
        if late:
            stats["missed"] += 1
            stats["missed_fraud"] += fraud
        elif fraud and two_hop(c["acct"]):
            stats["caught"] += 1            # linked to restricted accounts in two hops: hold or step up
        # The slower write path, judged on the same check: it would not hold the links if the payment
        # came sooner after the login than its freshness.
        if (c["ns"] - c["login_ns"]) / 1e9 < a.slow_path_s:
            stats["slow_missed"] += 1
            stats["slow_missed_fraud"] += fraud

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of links arrived inside the budget; partitions interleave,
                # so one fresh link alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier links this second", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            n = stats["checked"]
            pct = lambda k: 100 * stats[k] / n if n else 0
            print(f"[{a.name}] {r['n']:>7,} links/s  link-to-graph p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  accounts in graph {len(assets_of):,}", flush=True)
            print(f"[check] payment checks {n:,} (repeat fraudsters {stats['fraud']:,})  "
                  f"without the newest links {pct('missed'):.3f}%  repeat fraudsters linked to restricted accounts "
                  f"{stats['caught']:,} of {stats['fraud']:,}  at a {a.slow_path_s:g} s write path: without the newest links "
                  f"{pct('slow_missed'):.1f}% (repeat fraudsters {stats['slow_missed_fraud']:,})", flush=True)
            stats.clear()

    async def read(topic, handler, pids):
        # Each partition in turn, from the start of the log (the graph is rebuilt from it), then from this
        # reader's own position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
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

    await asyncio.gather(*readers(LINKS, on_link), *readers(CHECKS, on_check), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="graph")
    p.add_argument("--budget-ms", type=int, default=50, help="link-to-graph p99 budget for the lane")
    p.add_argument("--slow-path-s", type=float, default=SLOW_PATH_S, help="freshness of the write path to compare with")
    p.add_argument("--readers", type=int, default=2, help="connections reading each topic's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
