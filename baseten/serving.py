"""One KV cache-aware deployment: every model replica publishes each KV cache change onto the lane, and every
router applies it before it routes the next request that shares the prefix.

Plays one deployment in one process:
  * --replicas model replicas, each with a KV cache of --cache-blocks blocks (least recently used evicted first,
    leaf blocks before the prefix they extend). A request routed to a replica reuses its longest cached prefix,
    stores the rest and evicts what no longer fits; the replica publishes one STORED and, when it evicted
    anything, one REMOVED event to kv-events, keyed by deployment, with the replica's own event sequence.
  * --routers KV cache-aware routers. Each reads kv-events directly on its own connection and keeps its own
    index: block -> the replicas that hold it. For each request it scores every replica by prefix overlap
    against load (blocks to prefill plus --load-weight per request it has in flight there) and picks the
    cheapest. A router never sees another's index, so every change reaches every router through the log.
  * the traffic: --requests-per-s over --sessions long-context sessions (coding agents and chats) on --apps
    apps, each with its own system prompt; every turn sends the whole history again, a few blocks longer.
  * at --join-at s a new router starts (a scale-out or a restart): it rebuilds its index by replaying the lane
    from the start of the retained log, then routes like the others.

The business number: stale routes, requests a router sent to a replica for cached blocks that replica had
already evicted, because the removal had not reached that router yet, and the prefill tokens the replica then
computed again. Each request is also scored by a view of the cache --compare-ms behind, judged against the same
cache state before the request changes it, to show what a slower lane would cost. Prints every second with
event-to-router p50/p99/p99.9 against --budget-ms, gaps (must stay 0) and the cache hit rate.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import OrderedDict, defaultdict, deque
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, Partitioning, PollingStrategy, SendMessage

from common import (BLOCK_TOKENS, CLEARED, DEFAULT_URL, KV_EVENTS, PARTITIONS, REMOVED, STORED, STREAM, Window, connect,
                    ensure_topics, load, msg, part, replica)


class Replica:
    """A model replica's KV cache: block -> None, least recently used first."""

    def __init__(self, name, cap):
        self.name, self.cap, self.cache, self.seq = name, cap, OrderedDict(), 0

    def hit(self, chain):
        n = 0
        for b in chain:
            if b not in self.cache:
                break
            n += 1
        return n

    def serve(self, chain, n_hit):
        """Reuse the cached prefix, store the rest, evict what no longer fits. Returns (stored, removed)."""
        for b in reversed(chain):           # leaf first, so the prefix is the most recently used
            if b in self.cache:
                self.cache.move_to_end(b)
            else:
                self.cache[b] = None
        removed = []
        while len(self.cache) > self.cap:
            removed.append(self.cache.popitem(last=False)[0])
        return chain[n_hit:], removed


class Index:
    """A router's view of the deployment's KV cache: block -> replicas, plus each replica's last event."""

    def __init__(self):
        self.where, self.last, self.gaps, self.digest, self.pairs = {}, {}, 0, 0, 0

    def apply(self, d):
        r, seq = d["r"], d["q"]
        last = self.last.get(r, 0)
        if seq <= last:
            return False                    # a repeat: already applied
        if seq > last + 1:
            self.gaps += 1                  # an event from this replica never arrived
        self.last[r] = seq
        where = self.where
        if d["e"] == STORED:
            for b in d["b"]:
                s = where.get(b)
                if s is None:
                    where[b] = s = set()
                if r not in s:
                    s.add(r)
                    self.digest ^= hash((b, r))
                    self.pairs += 1
        elif d["e"] == CLEARED:
            for b in [b for b, s in where.items() if r in s]:
                where[b].discard(r)
                self.digest ^= hash((b, r))
                self.pairs -= 1
                if not where[b]:
                    del where[b]
        else:
            for b in d["b"]:
                s = where.get(b)
                if s is not None and r in s:
                    s.discard(r)
                    self.digest ^= hash((b, r))
                    self.pairs -= 1
                    if not s:
                        del where[b]
        return True

    def overlaps(self, chain):
        """Replica -> number of leading blocks of the chain it holds, by this view."""
        s = self.where.get(chain[0])
        if not s:
            return {}
        cur, ov = set(s), {}
        for i in range(1, len(chain)):
            nxt = self.where.get(chain[i])
            keep = cur & nxt if nxt else set()
            for r in cur - keep:
                ov[r] = i
            cur = keep
            if not cur:
                break
        for r in cur:
            ov[r] = len(chain)
        return ov


def pick(index, chain, busy, weight, n):
    """The cheapest replica: blocks it must prefill plus weight per request in flight there."""
    ov = index.overlaps(chain)
    best, cost = [], None
    for r in range(n):
        c = len(chain) - ov.get(r, 0) + weight * busy[r]
        if cost is None or c < cost:
            best, cost = [r], c
        elif c == cost:
            best.append(r)
    r = random.choice(best)
    return r, ov.get(r, 0)


async def main(a):
    await ensure_topics(await connect(a.url), KV_EVENTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await connect(a.url)).producer(STREAM, KV_EVENTS, topic_partitions_count=PARTITIONS[KV_EVENTS],
                                                     mode=cfg)
    pid = part(a.deployment, PARTITIONS[KV_EVENTS])
    start_ns = time.time_ns()
    t_start = time.monotonic()
    budget_ns = a.budget_ms * 1_000_000
    reps = [Replica(replica(i), a.cache_blocks) for i in range(a.replicas)]
    apps = [[f"a{i}:{j}" for j in range(random.randint(8, 24))] for i in range(a.apps)]   # system prompts
    sid = [0]

    def new_session():
        sid[0] += 1
        return [random.randrange(a.apps), sid[0], random.randint(40, 100), random.randint(5, 40)]

    sessions = [new_session() for _ in range(a.sessions)]   # [app, id, history blocks, turns left]
    outbox = []

    names = [f"router-{i:02d}" for i in range(a.routers)]
    late = f"router-{a.routers:02d}" if a.join_at else None
    view = {s: Index() for s in names}
    inflight = {s: [0] * a.replicas for s in names}       # each router's own requests in flight per replica
    live = {s: True for s in names}                        # routers caught up with the log
    nxt = {}                                               # router -> next offset it reads
    behind, behind_q = Index(), deque()                    # the view --compare-ms behind, fed from router-00's reads
    cmp_ns = int(a.compare_ms * 1e6)
    lat, st = Window(), defaultdict(float)
    catch = {}                                             # late router: replay progress
    loop = asyncio.get_running_loop()

    def route():
        k = random.randrange(len(sessions))
        s = sessions[k]
        app, i, n = s[0], s[1], s[2]
        s[2] += random.randint(1, 4)                       # the answer and the next message join the history
        s[3] -= 1
        if s[3] <= 0:
            sessions[k] = new_session()
        chain = apps[app] + [f"s{i}:{j}" for j in range(n)]
        who = random.choice([r for r in view if live[r]])
        r, claimed = pick(view[who], chain, inflight[who], a.load_weight, a.replicas)
        rep = reps[r]
        actual = rep.hit(chain)                            # the replica's real cache, before this request changes it
        if actual < claimed:
            st["stale"] += 1                               # sent here for blocks this replica had already evicted
            st["stale_tok"] += (claimed - actual) * BLOCK_TOKENS
        # The same request scored by the view --compare-ms behind, judged against the same cache state.
        rb, cb = pick(behind, chain, inflight[who], a.load_weight, a.replicas)
        ab = reps[rb].hit(chain)
        if ab < cb:
            st["stale_b"] += 1
            st["stale_b_tok"] += (cb - ab) * BLOCK_TOKENS
        st["req"] += 1
        st["tok"] += len(chain) * BLOCK_TOKENS
        st["hit_tok"] += actual * BLOCK_TOKENS
        inflight[who][r] += 1
        loop.call_later(a.gen_s, lambda w=who, x=r: inflight[w].__setitem__(x, inflight[w][x] - 1))
        stored, removed = rep.serve(chain, actual)
        now = time.time_ns()
        if stored:
            rep.seq += 1
            outbox.append(SendMessage(msg(r=r, q=rep.seq, e=STORED, b=stored,
                                          p=chain[actual - 1] if actual else None, ns=now)))
        if removed:
            rep.seq += 1
            outbox.append(SendMessage(msg(r=r, q=rep.seq, e=REMOVED, b=removed, ns=now)))
            st["evicted"] += len(removed)

    async def traffic():
        t0 = due = time.monotonic()
        owed = 0.0
        async with producer:
            while True:
                owed += a.requests_per_s * a.tick_ms / 1000
                for _ in range(int(owed)):
                    route()
                owed -= int(owed)
                if outbox:
                    batch = outbox[:]
                    outbox.clear()
                    st["events"] += len(batch)
                    await producer.send_with_partitioning(batch, Partitioning.partition_id(pid))
                due += a.tick_ms / 1000
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    def reader(s, joined_ns):
        async def run():
            c = await connect(a.url)
            who = Consumer.Single(f"{s}-{os.getpid()}")
            nxt[s] = None
            while True:
                # A rebuilding router reads in smaller batches so the routers already live keep their turn.
                n = 1000 if live[s] else 200
                ms = await c.poll_messages(STREAM, KV_EVENTS, consumer=who, partition_id=pid, count=n,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[s]) if nxt[s] is not None
                                           else PollingStrategy.First())
                if not ms:
                    await asyncio.sleep(a.poll_ms / 1000)
                    continue
                nxt[s] = ms[-1].offset() + 1
                now = time.time_ns()
                for m in ms:
                    d = load(m.payload())
                    if not view[s].apply(d):
                        continue
                    if s == names[0]:
                        behind_q.append(d)
                    if not live[s]:
                        # Still rebuilding: events made before this router started (or while it replayed) build
                        # the index but are not live latency. Track this second's oldest to know when it is done.
                        c_ = catch[s]
                        c_["n"] += 1
                        c_["sec_n"] += 1
                        c_["sec_age"] = max(c_["sec_age"], now - d["ns"])
                        if now - d["ns"] >= budget_ns:
                            c_["last_old"] = time.monotonic()
                        continue
                    if d["ns"] <= joined_ns:
                        continue            # replayed at start-up: applied, not timed
                    lat.add((now - d["ns"]) / 1e6)
                if len(ms) == n:
                    await asyncio.sleep(0)  # more is waiting: read on at once, after the others' turn
        return run

    async def compare_view():
        # Apply router-00's events to the slower view once they are --compare-ms old.
        while True:
            cut = time.time_ns() - cmp_ns
            while behind_q and behind_q[0]["ns"] <= cut:
                behind.apply(behind_q.popleft())
            await asyncio.sleep(0.005)

    async def join_late():
        await asyncio.sleep(a.join_at)
        view[late], inflight[late], live[late] = Index(), [0] * a.replicas, False
        catch[late] = dict(n=0, sec_n=0, sec_age=0, last_old=0.0, t=time.monotonic(), told=False)
        print(f"[routers] {late} starts at {a.join_at:g} s and rebuilds its index from the log", flush=True)
        await reader(late, time.time_ns())()

    async def report():
        print(f"[routers] deployment {a.deployment}: {a.replicas} replicas x {a.cache_blocks:,} KV blocks "
              f"({BLOCK_TOKENS} tokens each), {a.routers} routers, {a.sessions:,} sessions on {a.apps} apps, "
              f"{a.requests_per_s:g} requests/s", flush=True)
        while True:
            await asyncio.sleep(1)
            for s, c_ in catch.items():
                if live[s]:
                    if not c_["told"] and nxt.get(s) is not None and nxt[s] == nxt.get(names[0]):
                        # Same offset, same events applied: the rebuilt index must equal a router that never stopped.
                        v, w = view[s], view[names[0]]
                        same = v.digest == w.digest and v.pairs == w.pairs and v.last == w.last
                        print(f"[routers] {s} index {'identical to' if same else 'DIFFERS from'} {names[0]}'s at "
                              f"offset {nxt[s]:,}: {v.pairs:,} block copies on {len(v.where):,} blocks", flush=True)
                        c_["told"] = True
                    continue
                if c_["sec_n"] and c_["sec_age"] < budget_ns:
                    took = max(0.0, (c_["last_old"] or c_["t"]) - c_["t"])
                    live[s] = True
                    print(f"[routers] {s} rebuilt from the log in {took:.2f}s: {c_['n']:,} events replayed, "
                          f"{len(view[s].where):,} blocks known; now routing", flush=True)
                else:
                    print(f"[routers] {s} catching up: {c_['n']:,} events replayed", flush=True)
                c_["sec_n"] = c_["sec_age"] = 0
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            hit = 100 * st["hit_tok"] / max(st["tok"], 1)
            gaps = sum(v.gaps for v in view.values())
            print(f"[routers] {int(st['req']):>6,} requests/s  hit {hit:4.1f}% of prompt tokens  "
                  f"kv events {int(st['events']):,}/s (evicted {int(st['evicted']):,} blocks)  "
                  f"event-to-routers n {r['n']:,} p50 {r['p50']:.2f}ms p99 {r['p99']:.2f}ms p99.9 {r['p999']:.2f}ms "
                  f"{ok} (<{a.budget_ms:g}ms)  stale routes {int(st['stale']):,} ({int(st['stale_tok']):,} tokens "
                  f"prefilled again; {a.compare_ms:g} ms behind: {int(st['stale_b']):,}, {int(st['stale_b_tok']):,} "
                  f"tokens)  gaps {gaps}  routers {sum(live.values())}  t {time.monotonic() - t_start:.0f}s",
                  flush=True)
            st.clear()

    tasks = [traffic(), report(), compare_view()] + [reader(s, start_ns)() for s in names]
    if late:
        tasks.append(join_late())
    gc.freeze()                             # keep start-up objects out of the collector's way
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--deployment", default="qwen3-coder-480b", help="the model deployment; its partition key")
    p.add_argument("--replicas", type=int, default=16, help="model replicas in the deployment")
    p.add_argument("--cache-blocks", type=int, default=24_000, help="KV cache blocks per replica")
    p.add_argument("--routers", type=int, default=4, help="KV cache-aware routers, each with its own reader")
    p.add_argument("--sessions", type=int, default=6_000, help="long-context sessions sending turns")
    p.add_argument("--apps", type=int, default=60, help="apps, each with its own system prompt")
    p.add_argument("--requests-per-s", type=float, default=600, help="requests per second, all routers")
    p.add_argument("--gen-s", type=float, default=1.5, help="seconds a request stays in flight after routing")
    p.add_argument("--load-weight", type=float, default=1.0, help="blocks of prefill one in-flight request is worth")
    p.add_argument("--budget-ms", type=float, default=20, help="p99 budget, KV event made to applied by a router")
    p.add_argument("--compare-ms", type=float, default=1000, help="also score each request by a view this far behind")
    p.add_argument("--join-at", type=float, default=30, help="seconds until a new router joins (0 = none)")
    p.add_argument("--tick-ms", type=float, default=5, help="request loop tick; a tick's events go in one batch")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between polls when nothing is waiting")
    asyncio.run(main(p.parse_args()))
    os._exit(0)
