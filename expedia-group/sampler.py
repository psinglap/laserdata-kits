"""A Sampler: picks an arm for every bandit on a traveler's page, and keeps a session's picks consistent.

Each process is one Sampler pod; run several. The load balancer is not sticky, so a traveler's next page can land
on any pod. Every Sampler reads every partition of bandit-decisions directly (its own offsets, no consumer group,
nothing committed) on its own connection, so each one holds every live session's decisions in its consistency
cache. On start it rebuilds that cache from the log (the last --session-ttl-s seconds) before it takes a single
page request.

Page requests arrive on page-requests. Each Sampler owns a share of its partitions (--shard of --shards) and reads
them directly too; on start it takes requests from that moment on. For each property on the page it looks up the
session's decision in the cache; if there is none, it draws from the property's hero-image bandit (Thompson
Sampling on click-through rate), shows that arm, keeps it, and publishes the decision on the lane. Decisions and
picks go out from a separate task every 3 ms, so the read loops never wait on a send. A health beat every 200 ms
tells the load balancer the pod is ready.

For comparison, each Sampler also answers from a view of its cache that sees other pods' decisions only as of the
last sync, every --refresh-s seconds, and records that pick too; travelers.py checks both against what the
traveler saw. Both picks use the same random draw, so they differ only where the two caches differ.

Prints decision-to-cache latency every second (p50, p99, p99.9) with PASS or FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict
from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DECISIONS, DEFAULT_URL, PARTITIONS, PODS, REQUESTS, RESPONSES, STREAM, Window, arms_of,
                    connect, ensure_topics, load, msg, part_of, thompson)


class Cache:
    """Every live session's decisions: session -> {bandit: [arm, picked ns, applied at (monotonic), own]}."""

    def __init__(self):
        self.s = {}
        self.last = {}                    # session -> monotonic time of its last decision

    def apply(self, d, now_m, own=False) -> bool:
        ses = self.s.setdefault(d["k"], {})
        self.last[d["k"]] = now_m
        v = ses.get(d["b"])
        if v is not None and (v[1], v[0]) <= (d["ns"], d["arm"]):
            return False                  # first decision wins everywhere; a repeat or replay changes nothing
        ses[d["b"]] = [d["arm"], d["ns"], now_m, own]
        return True

    def prune(self, older_than):
        for k in [k for k, t in self.last.items() if t < older_than]:
            del self.last[k]
            self.s.pop(k, None)


async def main(a):
    me = f"{a.name}.{os.getpid()}.{os.urandom(3).hex()}"   # unique per start, so a restart replays its own past
    await ensure_topics(await connect(a.url), DECISIONS, REQUESTS, RESPONSES, PODS)
    reader, writer = await connect(a.url), await connect(a.url)       # one connection each way
    P = PARTITIONS[DECISIONS]
    start_ns = time.time_ns()

    cache = Cache()
    params = {}                           # property -> its bandit's parameters (the Bandit Store's copy)
    synced = [0.0]                        # the comparison view's last sync (monotonic)
    rng = random.Random(os.getpid())
    out_dec, out_pick = [], []            # waiting for the sender task
    lat, st, run = Window(), defaultdict(int), defaultdict(int)
    caught = [None]

    async def read_lane():
        who = {p: Consumer.Single(f"{me}-{p}") for p in range(P)}
        since = PollingStrategy.Timestamp((time.time_ns() - int(a.session_ttl_s * 1e9)) // 1000)
        nxt, done = {}, set()
        last = time.monotonic()
        while True:
            now_m = time.monotonic()
            st["gap_ms"] = max(st["gap_ms"], 1000 * (now_m - last))    # longest pass over all partitions
            last, more = now_m, False
            for p in range(P):
                ms = await reader.poll_messages(STREAM, DECISIONS, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                now = time.time_ns()
                now_m = time.monotonic()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        d = load(m.payload())
                        if d["by"] == me:
                            continue        # this pod's own decision: already in its cache
                        if caught[0] is not None and d["ns"] >= start_ns:
                            lat.add((now - d["ns"]) / 1e6)      # another pod's live decision, not the rebuild
                            st["decisions"] += 1
                        else:
                            run["replayed"] += 1
                        if not cache.apply(d, now_m):
                            run["repeats"] += 1
                if len(ms) == a.batch:
                    more = True             # a rebuild or a burst: read on at once
                elif p not in done:
                    done.add(p)             # this partition has been read to the end of the log once
                    if len(done) == P:
                        caught[0] = now_m
                        synced[0] = now_m
                        gc.freeze()         # keep the rebuilt cache out of the collector's way
                        n = sum(len(v) for v in cache.s.values())
                        print(f"[{a.name}] rebuilt the cache from the log in {(now - start_ns) / 1e9:.2f}s: "
                              f"{len(cache.s):,} sessions, {n:,} decisions, {run['replayed']:,} replayed",
                              flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def sync_view():
        # The comparison view: other pods' decisions as of the last sync, every --refresh-s seconds.
        while caught[0] is None:
            await asyncio.sleep(0.05)
        due = time.monotonic()
        while True:
            due += a.refresh_s
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            synced[0] = time.monotonic()
            cache.prune(time.monotonic() - a.session_ttl_s)

    def on_request(m):
        r = load(m.payload())
        if time.time_ns() - r["ns"] > a.expire_s * 1e9:
            st["expired"] += 1              # the traveler's page has already timed out
            return
        ses = cache.s.get(r["s"], {})
        now_m = time.monotonic()
        picks = []
        for b in r["b"]:
            v = ses.get(b) if ses else cache.s.get(r["s"], {}).get(b)
            live = v[0] if v else None
            alt = v[0] if v and (v[3] or v[2] <= synced[0]) else None   # own decisions, or synced ones
            if live is None or alt is None:
                if b not in params:
                    params[b] = arms_of(b)
                draw = thompson(params[b], rng)     # one draw, used by both views
                if live is None:
                    live = draw
                    d = {"k": r["s"], "b": b, "arm": draw, "by": me, "ns": time.time_ns()}
                    cache.apply(d, now_m, own=True)
                    out_dec.append((r["s"], SendMessage(msg(**d))))
                    st["new"] += 1
                if alt is None:
                    alt = draw
            else:
                st["kept"] += 1
            picks.append([b, live, alt])
        st["pages"] += 1
        out_pick.append(SendMessage(msg(id=r["id"], s=r["s"], p=picks, by=a.name)))

    async def send_out():
        while True:
            await asyncio.sleep(0.003)
            if out_dec or out_pick:
                dec, out_dec[:] = list(out_dec), []
                pk, out_pick[:] = list(out_pick), []
                by_p = defaultdict(list)    # one batch per partition: a session's decisions stay in order
                for k, m in dec:
                    by_p[part_of(k)].append(m)
                sends = [writer.send_messages(STREAM, DECISIONS, Partitioning.partition_id(p), ms)
                         for p, ms in by_p.items()]
                if pk:
                    sends.append(writer.send_messages(STREAM, RESPONSES, Partitioning.balanced(), pk))
                await asyncio.gather(*sends)

    async def health():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # not ready until the cache is rebuilt
        hc = await connect(a.url)
        due = time.monotonic()
        while True:
            await hc.send_messages(STREAM, PODS, Partitioning.partition_id(0),
                                   [SendMessage(msg(pod=a.name, shard=a.shard, shards=a.shards))])
            due += 0.2
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def take_requests():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never answer before the cache is rebuilt
        rq = await connect(a.url)           # its own connection, so pages never queue behind the lane
        mine = [p for p in range(PARTITIONS[REQUESTS]) if p % a.shards == a.shard]
        who = {p: Consumer.Single(f"{me}-req-{p}") for p in mine}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)     # pages from now on
        nxt = {}
        while True:
            more = False
            for p in mine:
                ms = await rq.poll_messages(STREAM, REQUESTS, consumer=who[p], partition_id=p, count=a.batch,
                                            auto_commit=False,
                                            polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_request(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the cache from the log: {run['replayed']:,} decisions so far", flush=True)
                lat.flush()
                st.clear()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {st['decisions']:>6,} decisions/s  decision-to-cache p50 {r['p50']:5.1f}ms  "
                  f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"pages {st['pages']:,}  new picks {st['new']:,}  kept {st['kept']:,}  expired {st['expired']:,}  "
                  f"cache {len(cache.s):,} sessions  read pass max {st['gap_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(read_lane(), sync_view(), take_requests(), send_out(), health(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="sampler-1")
    p.add_argument("--budget-ms", type=float, default=50, help="decision-to-cache p99 budget")
    p.add_argument("--shard", type=int, default=0, help="this Sampler's share of page-requests: 0 .. shards-1")
    p.add_argument("--shards", type=int, default=1, help="how many Samplers share the page requests")
    p.add_argument("--refresh-s", type=float, default=1, help="sync period of the comparison view")
    p.add_argument("--session-ttl-s", type=float, default=1800, help="a session idle this long has ended")
    p.add_argument("--expire-s", type=float, default=2.0, help="a page request older than this has timed out")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
