"""The session context service: each shopper's short-term context, kept current from the lane, and every
type-ahead request boosted from it.

Two readers, each reading every partition of its topic directly on connections of its own (no consumer
group: a group reader visits one partition per poll, so with 16 partitions a record waits its turn):
  * shopper-actions (the lane): folds each action into the shopper's session context (annotations
    with weights that grow with every signal and decay with time), ignoring any older or repeated
    sequence. Action-to-context latency is timed here.
  * typeahead-requests: plays the type-ahead service. It boosts the suggestions from the context (the
    strongest category first) and asks: did the context already hold the shopper's last action when the
    request started? If not, the suggestions were boosted from a stale context, and if the stale context
    leads with a different category, the shopper saw suggestions for what they wanted a moment ago.

--hold-ms is a what-if: apply each action only once it is that old, as a batching path would.
Prints action-to-context p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the
business numbers: type-ahead requests boosted without the shopper's last action, and wrong categories boosted.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import ACTIONS, DEFAULT_URL, PARTITIONS, STREAM, TYPEAHEAD, Window, ensure_topics, fold, load, top


async def main(a):
    async def own_client():                 # one connection per reader, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ACTIONS, TYPEAHEAD)

    ctx = {}                                # user -> [session, last action ns, {annotation: weight}]
    seen = {}                               # user -> [seq, when applied (ns), last seen (monotonic)]
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays actions made before it joined
    held = deque()                          # what-if only: actions waiting until they are --hold-ms old
    gc.freeze()

    def apply(v):
        u = v["user"]
        s = seen.get(u)
        if s and v["seq"] <= s[0]:
            stats["dropped"] += 1           # older or repeated action: already applied
            return
        ctx[u] = fold(ctx.get(u), v["sess"], v["ann"], v["ns"])
        now = time.time_ns()
        if s is None:
            seen[u] = s = [0, 0, 0.0]
        s[0], s[1], s[2] = v["seq"], now, time.monotonic()
        age = (now - v["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest action seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_action(m):
        v = load(m.payload())
        if a.hold_ms:
            held.append(v)
        else:
            apply(v)

    async def release():                    # what-if only: apply held actions once they are old enough
        while True:
            cut = time.time_ns() - a.hold_ms * 1_000_000
            while held and held[0]["ns"] <= cut:
                apply(held.popleft())
            await asyncio.sleep(0.002)

    async def on_request(m):
        r = load(m.payload())
        if not caught_up[0] or time.time_ns() - r["ns"] > 5e9:
            return                          # replayed history, not a request happening now
        seq, applied, _ = seen.get(r["user"], (0, 0, 0))
        boost = top(ctx.get(r["user"]))     # the category the suggestions lead with
        stats["requests"] += 1
        stats["moved"] += r["moved"]
        # The request started at r["ns"]. Had the context applied the shopper's last action by then?
        # (Both clocks are wall clocks; across machines, keep them NTP-synced.)
        if seq < r["needs"] or (seq == r["needs"] and applied > r["ns"]):
            stats["stale"] += 1
            if boost != r["top"]:           # suggestions lead with what the shopper wanted a moment ago
                stats["wrong"] += 1

    async def sweep():
        while True:
            await asyncio.sleep(30)         # forget shoppers idle past a session (--keep-s)
            cut = time.monotonic() - a.keep_s
            for u in [u for u, s in seen.items() if s[2] < cut]:
                del seen[u]
                ctx.pop(u, None)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of actions arrived inside the budget; partitions interleave,
                # so one fresh action alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms + a.hold_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier actions this second", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms + a.hold_ms else "FAIL"
            stale = 100 * stats["stale"] / stats["requests"] if stats["requests"] else 0
            print(f"[{a.name}] {r['n']:>7,} actions/s  action-to-context p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms + a.hold_ms}ms)  shoppers in context {len(ctx):,}",
                  flush=True)
            print(f"[type-ahead] requests {stats['requests']:,}  after an intent change {stats['moved']:,}  "
                  f"boosted without the last action {stale:.3f}%  wrong category boosted {stats['wrong']:,}", flush=True)
            stats.clear()

    async def read(topic, handler, pids):
        # Each partition in turn, from the start of the log (the context is rebuilt from it), then from this
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

    await asyncio.gather(*readers(ACTIONS, on_action), *readers(TYPEAHEAD, on_request), release(), sweep(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="context")
    p.add_argument("--budget-ms", type=int, default=50, help="action-to-context p99 budget for the lane")
    p.add_argument("--hold-ms", type=int, default=0, help="what-if: apply each action only once it is this old")
    p.add_argument("--readers", type=int, default=2, help="connections reading each topic's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--keep-s", type=float, default=3600, help="seconds to keep an idle shopper's context (a session)")
    asyncio.run(main(p.parse_args()))
