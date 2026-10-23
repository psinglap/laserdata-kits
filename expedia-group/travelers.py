"""The travelers and the load balancer: sessions that search, compare and open properties, page after page.

Each session picks a destination and views pages: a search results page (--per-page properties), the same search
re-sorted or filtered (mostly the same properties), the next results page, or one property's details page. Every
property on a page carries a hero image chosen by that property's bandit. Between pages a traveler pauses for a
think time (median about 2 s; a back button or a quick tap is a few hundred ms).

The load balancer sends each page request to a random healthy Sampler (one with a health beat in the last 0.5 s),
never the same one on purpose: there is no stickiness. It reads every Sampler's picks back and checks them: a
property the traveler has already seen in this session must show the same image again. A different image is a
flipped image. Each pick also carries the image the same Sampler would have shown from a cache synced every few
seconds (--refresh-s on sampler.py); that pick is checked against the same thing the traveler saw before this page.

--surge multiplies new sessions for --surge-for seconds from --surge-at, an illustrative spike in traffic.
"""
import argparse
import asyncio
import gc
import heapq
import itertools
import math
import random
import time
from collections import defaultdict
from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, PARTITIONS, PODS, REQUESTS, RESPONSES, STREAM, Window, connect, ensure_topics,
                    load, msg, property_id)


async def main(a):
    await ensure_topics(await connect(a.url), REQUESTS, RESPONSES, PODS)
    req_w, resp_r, pod_r = await connect(a.url), await connect(a.url), await connect(a.url)

    rng = random.Random(7)
    per_dest = max(a.per_page * 3, a.properties // a.destinations)
    dest_w = [1.0 / (i + 1) ** 0.8 for i in range(a.destinations)]    # a few destinations draw most travelers
    sid = itertools.count(1)
    rid = itertools.count(1)
    sessions = {}                 # session -> [destination, results offset, last results, pages left]
    seen = {}                     # session -> {property: image the traveler saw last}
    due = []                      # heap of (time of the next page, session)
    pending = {}                  # request id -> (sent ns, session, sampler partition)
    pods = {}                     # Sampler name -> (shard, shards, last beat monotonic)
    st, lat = defaultdict(int), Window()
    run = {"surge": False}
    mean_pages = a.pages

    def results(dest, offset):
        base = dest * per_dest
        return [property_id(base + (offset + i) % per_dest) for i in range(a.per_page)]

    def think():
        return min(30.0, max(0.12, rng.lognormvariate(math.log(a.think_s), 0.9)))

    def next_page(s):
        v = sessions[s]
        kind = rng.random()
        if v[2] is None:                              # first page: a search
            v[2] = results(v[0], 0)
            return "search", v[2]
        if kind < 0.45:                               # open one property from the results
            return "property", [rng.choice(v[2])]
        if kind < 0.75:                               # re-sort or filter: mostly the same properties
            keep = rng.sample(v[2], k=max(1, int(a.per_page * 0.7)))
            v[1] += a.per_page
            v[2] = keep + [x for x in results(v[0], v[1]) if x not in keep][:a.per_page - len(keep)]
            rng.shuffle(v[2])
            return "search", v[2]
        v[1] += a.per_page                            # the next results page
        v[2] = results(v[0], v[1])
        return "search", v[2]

    def healthy_partitions(now_m):
        out = []
        for name, (shard, shards, beat) in pods.items():
            if now_m - beat < 0.5:
                out += [p for p in range(PARTITIONS[REQUESTS]) if p % shards == shard]
        return out

    async def read_health():
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)
        who, nxt = Consumer.Single("lb-health"), None
        while True:
            ms = await pod_r.poll_messages(STREAM, PODS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else since)
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    h = load(m.payload())
                    pods[h["pod"]] = (h["shard"], h["shards"], time.monotonic())
            await asyncio.sleep(0.05)

    async def run_travelers():
        while not pods:
            await asyncio.sleep(0.1)                  # wait for the first Sampler to be ready
        gc.freeze()
        t0 = tick = time.monotonic()
        owed = 0.0
        while True:
            now_m = time.monotonic()
            t = now_m - t0
            surge = a.surge > 1 and a.surge_at <= t < a.surge_at + a.surge_for
            run["surge"] = surge
            owed += a.pages_per_s / mean_pages / 100 * (a.surge if surge else 1)   # new sessions this tick
            for _ in range(int(owed)):
                s = f"S{next(sid):08d}"
                d = rng.choices(range(a.destinations), dest_w)[0]
                sessions[s] = [d, 0, None, 1 + int(rng.expovariate(1 / max(0.5, mean_pages - 0.5)))]   # mean --pages
                seen[s] = {}
                heapq.heappush(due, (now_m, s))
                st["sessions"] += 1
            owed -= int(owed)
            parts = healthy_partitions(now_m)
            by_p, now_ns = defaultdict(list), time.time_ns()
            while due and due[0][0] <= now_m:
                _, s = heapq.heappop(due)
                v = sessions[s]
                kind, props = next_page(s)
                v[3] -= 1
                if not parts:
                    st["no_sampler"] += 1
                else:
                    p = rng.choice(parts)
                    r = f"r{next(rid)}"
                    pending[r] = (now_ns, s, p)
                    by_p[p].append(SendMessage(msg(id=r, s=s, pg=kind, b=props, ns=now_ns)))
                    st["requests"] += 1
                if v[3] > 0:
                    heapq.heappush(due, (now_m + think(), s))
                else:
                    sessions.pop(s)                   # the session ends; its picks are checked as they arrive
            t_send = time.monotonic()
            if by_p:
                await asyncio.gather(*(req_w.send_messages(STREAM, REQUESTS, Partitioning.partition_id(p), ms)
                                       for p, ms in by_p.items()))
            st["send_ms"] = max(st["send_ms"], 1000 * (time.monotonic() - t_send))
            st["late_ms"] = max(st["late_ms"], 1000 * (t_send - tick))   # how late this tick started
            tick += 0.01
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def read_picks():
        who = {p: Consumer.Single(f"lb-picks-{p}") for p in range(PARTITIONS[RESPONSES])}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)
        nxt = {}
        while True:
            more = False
            for p in who:
                ms = await resp_r.poll_messages(STREAM, RESPONSES, consumer=who[p], partition_id=p, count=1000,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_picks(m)
                    more = more or len(ms) == 1000
            if not more:
                await asyncio.sleep(0.001)

    def on_picks(m):
        r = load(m.payload())
        req = pending.pop(r["id"], None)
        if req is None:
            return                         # answered twice (at-least-once) or already timed out
        lat.add((time.time_ns() - req[0]) / 1e6)
        st["pages"] += 1
        sv = seen.get(r["s"])
        if sv is None:
            return
        for b, live, alt in r["p"]:
            before = sv.get(b)             # judge both picks against what the traveler saw before this page
            if before is not None:
                st["revisits"] += 1
                if live != before:
                    st["flipped"] += 1
                if alt != before:
                    st["alt_flipped"] += 1
            sv[b] = live
            st["shown"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            cutoff = time.time_ns() - 5_000_000_000
            for r in [r for r, v in pending.items() if v[0] < cutoff]:
                del pending[r]
                st["unanswered"] += 1      # no picks within 5 s: the page rendered without them
            ended = [s for s in seen if s not in sessions]
            if len(ended) > 50_000:
                for s in ended[:-20_000]:
                    del seen[s]
            r = lat.flush()
            rv = max(1, st["revisits"])
            healthy = sum(1 for v in pods.values() if time.monotonic() - v[2] < 0.5)
            print(f"[travelers] pages requested {st['requests']:,} answered {st['pages']:,} "
                  f"({'surge' if run['surge'] else 'steady'})  images shown {st['shown']:,}  "
                  f"revisits {st['revisits']:,}  flipped {st['flipped']:,} ({100 * st['flipped'] / rv:.2f}%)  "
                  f"a cache synced every {a.refresh_s:g}s would flip {st['alt_flipped']:,} "
                  f"({100 * st['alt_flipped'] / rv:.2f}%)  "
                  f"new sessions {st['sessions']:,}  live sessions {len(sessions):,}  healthy Samplers {healthy}  "
                  f"unanswered {st['unanswered']:,}  no Sampler {st['no_sampler']:,}  "
                  f"page-to-picks p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  "
                  f"tick late max {st['late_ms']:.0f}ms  send max {st['send_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(read_health(), run_travelers(), read_picks(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--properties", type=int, default=50_000, help="properties with a hero-image bandit")
    p.add_argument("--destinations", type=int, default=500)
    p.add_argument("--per-page", type=int, default=10, help="properties on a search results page")
    p.add_argument("--pages-per-s", type=float, default=600, help="page views per second, all sessions")
    p.add_argument("--pages", type=float, default=6, help="mean pages per session")
    p.add_argument("--think-s", type=float, default=2.0, help="median pause between a traveler's pages")
    p.add_argument("--surge", type=float, default=1, help="new-session multiplier in the spike (1 = off)")
    p.add_argument("--surge-at", type=float, default=60, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=30)
    p.add_argument("--refresh-s", type=float, default=1, help="the comparison cache's sync, as set on sampler.py (label only)")
    asyncio.run(main(p.parse_args()))
