"""The online learner: each user's decision context and every action's reward, kept current from the lane,
and every next decision request answered from it.

Two readers, each reading every partition of its topic directly on connections of its own (no consumer
group: a group reader visits one partition per poll, so with 16 partitions a record waits its turn):
  * decision-outcomes (the lane): folds each delivered decision and each goal event into the user's
    context (cart open, checked out, uninstalled, cancelled, touches today), ignoring any older or
    repeated sequence, and credits each goal event inside the horizon to the action that preceded it
    (the (x, a, r) record that updates the learner). Outcome-to-learner latency is timed here.
  * decision-requests: plays the next request for that user. It decides from the context (the
    highest-scored feasible action) and asks: did the learner already hold the user's last record when
    the request started? If not, the decision was made without the last outcome, and if that changes the
    action, the engine chose wrongly: a cart reminder after the checkout, a push to an app that was just
    uninstalled, or a fourth message past the cap.

--hold-ms is a what-if: apply each record only once it is that old, as a batching path would.
Prints outcome-to-learner p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the
business numbers: decisions made without the user's last outcome, and wrong actions chosen.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import (DEFAULT_URL, HORIZON_S, OUTCOMES, PARTITIONS, REQUESTS, REWARD, STREAM, Window, decide,
                    ensure_topics, fold, load)


async def main(a):
    async def own_client():                 # one connection per reader, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), OUTCOMES, REQUESTS)

    ctx = {}                                # user -> decision context (common.fold)
    seen = {}                               # user -> [seq, when applied (ns), last seen (monotonic)]
    arms = defaultdict(lambda: [0, 0])      # action -> [goal events credited, reward sum]
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays outcomes from before it joined
    held = deque()                          # what-if only: records waiting until they are --hold-ms old
    gc.freeze()

    def apply(v):
        u = v["user"]
        s = seen.get(u)
        if s and v["seq"] <= s[0]:
            stats["dropped"] += 1           # older or repeated record: already applied
            return
        st = ctx.get(u)
        kind = v["kind"]
        if kind in REWARD and st and st[6] and 0 <= v["ns"] - st[6] <= HORIZON_S * 1e9:
            arm = arms[st[5]]               # reward r for the decision (x, a) that came before it
            arm[0] += 1
            arm[1] += REWARD[kind]
            stats["credited"] += 1
        ctx[u] = fold(st, kind, v.get("action", ""), v["ns"])
        now = time.time_ns()
        if s is None:
            seen[u] = s = [0, 0, 0.0]
        s[0], s[1], s[2] = v["seq"], now, time.monotonic()
        age = (now - v["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_outcome(m):
        v = load(m.payload())
        if a.hold_ms:
            held.append(v)
        else:
            apply(v)

    async def release():                    # what-if only: apply held records once they are old enough
        while True:
            cut = time.time_ns() - a.hold_ms * 1_000_000
            while held and held[0]["ns"] <= cut:
                apply(held.popleft())
            await asyncio.sleep(0.002)

    async def on_request(m):
        r = load(m.payload())
        if not caught_up[0] or time.time_ns() - r["ns"] > 5e9:
            return                          # replayed history, not a request happening now
        u = r["user"]
        seq, applied, _ = seen.get(u, (0, 0, 0))
        pick = decide(u, ctx.get(u))        # the action the engine chooses now
        stats["requests"] += 1
        stats["moved"] += r["moved"]
        # The request started at r["ns"]. Had the learner applied the user's last record by then?
        # (Both clocks are wall clocks; across machines, keep them NTP-synced.)
        if seq < r["needs"] or (seq == r["needs"] and applied > r["ns"]):
            stats["stale"] += 1
            if pick != r["next"]:           # e.g. a cart reminder after the checkout, or past the cap
                stats["wrong"] += 1
                stats["nothing_due"] += r["next"] == "no_action"   # past the cap, or nobody left to reach

    async def sweep():
        while True:
            await asyncio.sleep(30)         # forget users idle past a day's context (--keep-s)
            cut = time.monotonic() - a.keep_s
            for u in [u for u, s in seen.items() if s[2] < cut]:
                del seen[u]
                ctx.pop(u, None)

    async def report():
        while True:
            await asyncio.sleep(1)
            # Move what survived the last second (contexts, users) out of the collector's reach, so a full
            # collection never walks hundreds of thousands of long-lived objects and pauses the readers.
            gc.freeze()
            if not caught_up[0]:
                # caught up once a whole second of records arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms + a.hold_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier outcomes this second", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms + a.hold_ms else "FAIL"
            stale = 100 * stats["stale"] / stats["requests"] if stats["requests"] else 0
            print(f"[{a.name}] {r['n']:>7,} outcomes/s  outcome-to-learner p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms + a.hold_ms}ms)  rewards credited "
                  f"{stats['credited']:,}  users in context {len(ctx):,}", flush=True)
            print(f"[next-decision] decisions {stats['requests']:,}  changed by the last outcome {stats['moved']:,}  "
                  f"decided without the last outcome {stale:.3f}%  wrong action chosen {stats['wrong']:,}  "
                  f"(sent when nothing was due {stats['nothing_due']:,})", flush=True)
            stats.clear()

    async def read(topic, handler, pids):
        # Each partition in turn, from the start of the log (contexts are rebuilt from it), then from this
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

    await asyncio.gather(*readers(OUTCOMES, on_outcome), *readers(REQUESTS, on_request), release(), sweep(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="learner")
    p.add_argument("--budget-ms", type=int, default=50, help="outcome-to-learner p99 budget for the lane")
    p.add_argument("--hold-ms", type=int, default=0, help="what-if: apply each record only once it is this old")
    p.add_argument("--readers", type=int, default=2, help="connections reading each topic's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--keep-s", type=float, default=86400, help="seconds to keep an idle user's context (a day)")
    asyncio.run(main(p.parse_args()))
