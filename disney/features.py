"""The live personalization feature service: what each profile is watching, kept current from the lane,
and every Live rail request built from it.

Two readers, each reading every partition of its topic directly on connections of its own (no consumer
group: a group reader visits one partition per poll, so with 16 partitions a record waits its turn):
  * watch-signals (the lane): sets the profile's now-watching event and the event it just left,
    ignoring any older or repeated sequence. Signal-to-feature latency is timed here.
  * live-rail-requests: plays the rail service. It builds the Live rail from the features (the event
    the profile is watching is marked "Watching", the one it just left becomes "Back to", the rest are
    offered as "Watch live") and asks: did the features already hold the profile's last signal when the
    request started? If not, the rail was built on stale features, and if the stale now-watching event
    differs from the real one, the viewer sees a wrong tile.

--hold-ms is a what-if: apply each signal only once it is that old, as a slower path would.
Prints signal-to-feature p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the
business numbers: rails built without the viewer's last signal, and wrong tiles shown.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, PARTITIONS, RAILS, STREAM, WATCH, Window, ensure_topics, load


async def main(a):
    async def own_client():                 # one connection per reader, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), WATCH, RAILS)

    feat = {}                               # profile -> [seq, when applied (ns), now watching, just left, last seen]
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays signals made before it joined
    held = deque()                          # what-if only: signals waiting until they are --hold-ms old
    gc.freeze()

    def apply(v):
        p = v["profile"]
        f = feat.get(p)
        if f and v["seq"] <= f[0]:
            stats["dropped"] += 1           # older or repeated signal: already applied
            return
        now = time.time_ns()
        if f is None:
            feat[p] = f = [0, 0, "", "", 0.0]
        f[0], f[1], f[2], f[3], f[4] = v["seq"], now, v["now"], v["left"], time.monotonic()
        age = (now - v["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest signal seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_watch(m):
        v = load(m.payload())
        if a.hold_ms:
            held.append(v)
        else:
            apply(v)

    async def release():                    # what-if only: apply held signals once they are old enough
        while True:
            cut = time.time_ns() - a.hold_ms * 1_000_000
            while held and held[0]["ns"] <= cut:
                apply(held.popleft())
            await asyncio.sleep(0.002)

    async def on_rail(m):
        r = load(m.payload())
        if not caught_up[0] or time.time_ns() - r["ns"] > 5e9:
            return                          # replayed history, not a request happening now
        seq, applied, now, left, _ = feat.get(r["profile"], (0, 0, "", "", 0))
        tiles = [("Watching" if e == now else "Back to" if e == left else "Watch live", e) for e in r["live"]]
        stats["requests"] += 1
        stats["tiles"] += len(tiles)
        # The request started at r["ns"]. Had the features applied the profile's last signal by then?
        # (Both clocks are wall clocks; across machines, keep them NTP-synced.)
        if seq < r["needs"] or (seq == r["needs"] and applied > r["ns"]):
            stats["stale"] += 1
            if now != r["now"]:             # "Watching" on the event they left, or "Watch live" on the one they're on
                stats["wrong"] += 1

    async def sweep():
        while True:
            await asyncio.sleep(30)         # forget profiles idle past the live window (--keep-s)
            cut = time.monotonic() - a.keep_s
            for p in [p for p, f in feat.items() if f[4] < cut]:
                del feat[p]

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of signals arrived inside the budget; partitions interleave,
                # so one fresh signal alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms + a.hold_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier signals this second", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms + a.hold_ms else "FAIL"
            stale = 100 * stats["stale"] / stats["requests"] if stats["requests"] else 0
            print(f"[{a.name}] {r['n']:>7,} signals/s  signal-to-feature p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms + a.hold_ms}ms)  profiles in view {len(feat):,}",
                  flush=True)
            print(f"[rail] Live rail requests {stats['requests']:,}  tiles {stats['tiles']:,}  "
                  f"built without the last signal {stale:.3f}%  wrong tiles shown {stats['wrong']:,}", flush=True)
            stats.clear()

    async def read(topic, handler, pids):
        # Each partition in turn, from the start of the log (the view is rebuilt from it), then from this
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

    await asyncio.gather(*readers(WATCH, on_watch), *readers(RAILS, on_rail), release(), sweep(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="features")
    p.add_argument("--budget-ms", type=int, default=50, help="signal-to-feature p99 budget for the lane")
    p.add_argument("--hold-ms", type=int, default=0, help="what-if: apply each signal only once it is this old")
    p.add_argument("--readers", type=int, default=2, help="connections reading each topic's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--keep-s", type=float, default=4 * 3600, help="seconds to keep an idle profile (a live window)")
    asyncio.run(main(p.parse_args()))
