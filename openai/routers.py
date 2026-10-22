"""A fleet of API request routers: every served request onto the lane, and every key disable applied before
that key's next request.

Plays --routers request routers in one process. Each one checks the API key of every request against its
own view of disabled keys and, if the key is still good, serves the request and publishes one record to
api-requests, keyed by key: router, key, project, the network the request came from, a per-(router, key)
sequence and its cost. Each router also reads key-status directly on its own connection and applies every
disable to its own view. A router never sees another's view, so a disable reaches every router through the log.

The same process plays the outside world, on fixed schedules (all illustrative):
  * the keys' own traffic: --requests-per-s over --keys keys, a tenth of the keys carrying most of it, each
    key from its owner's one or two networks;
  * a HIJACK every --hijack-every s: a stolen key starts serving --attack-rps requests a second from dozens
    of networks, for --attack-for s (the attacker keeps retrying after it is cut off);
  * a LEAK every --leak-every s: secret scanning finds a key in public code and reports it on key-reports;
    an attacker who scraped the same code starts using it --attacker-after s later.

The business number: requests served on a key after its disable was published, because the disable had not
reached that router yet, with their cost; and the same for a lane --compare-ms behind. Prints it every
second, with disable-to-router p50/p99: the second hop of the loop.
"""
import argparse
import asyncio
import gc
import os
import random
import statistics
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ABUSE_HIGH, ABUSE_LOW, COST_HIGH, COST_LOW, DEFAULT_URL, PARTITIONS, REPORTS, REQUESTS, STATUS,
                    STREAM, Window, ensure_topics, home_networks, key, load, msg, part, project)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), REQUESTS, REPORTS, STATUS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, REQUESTS, topic_partitions_count=PARTITIONS[REQUESTS],
                                                   mode=cfg)
    scanner = await (await own_client()).producer(STREAM, REPORTS, topic_partitions_count=PARTITIONS[REPORTS])
    start_ns = time.time_ns()
    routers = [f"router-{i:02d}" for i in range(a.routers)]
    keys = [key(i) for i in range(a.keys)]
    heavy = keys[: max(1, a.keys // 10)]
    pid = {k: part(k, PARTITIONS[REQUESTS]) for k in keys}
    nets = {k: home_networks(k) for k in keys}
    proj = {k: project(k) for k in keys}
    view = {s: {} for s in routers}         # router -> {key: disable ns}
    seq = defaultdict(int)                  # (router, key) -> sequence
    attacks = {}                            # key -> (start, end, networks): abusive traffic on a stolen key
    kind = {}                               # key -> "hijack" | "leak", for keys the outside world abused
    recent = defaultdict(lambda: deque(maxlen=2048))   # abused key -> (ns, served, usd) of recent attempts
    truth = {}                              # key -> disable ns, once the first router has it
    detect = []                             # hijack: first abusive request to disable published, ms
    reports = []                            # leak reports waiting for the scanner's send
    lat, stats = Window(), defaultdict(float)
    cmp_ns = a.compare_ms * 1_000_000

    def account(k, t, served, usd):
        """One attempt on an abused key, counted against its disable once that is known."""
        d = truth.get(k)
        if d is None:
            recent[k].append((t, served, usd))
            return
        if t <= d:
            return
        if t - d < cmp_ns:
            stats["would"] += 1             # a lane --compare-ms behind would have served it
            stats["would_usd"] += usd
        if served:
            stats["after"] += 1             # served after the disable was published
            stats["after_usd"] += usd

    def reader(s):
        async def run():
            c = await own_client()
            who, nxt = Consumer.Single(f"{s}-{os.getpid()}"), None
            while True:
                ms = await c.poll_messages(STREAM, STATUS, consumer=who, partition_id=0, count=1000,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                           else PollingStrategy.First())
                if not ms:
                    await asyncio.sleep(a.poll_ms / 1000)
                    continue
                nxt = ms[-1].offset() + 1
                for m in ms:
                    d = load(m.payload())
                    k = d["k"]
                    if k in view[s]:
                        continue            # older or repeated disable: already applied
                    view[s][k] = d["ns"]
                    if d["ns"] <= start_ns:
                        continue            # replayed at start-up: applied, not timed
                    lat.add((time.time_ns() - d["ns"]) / 1e6)
                    if k not in truth:
                        truth[k] = d["ns"]
                        stats["disables"] += 1
                        if k not in kind:
                            stats["unplanted"] += 1     # a disable for a key nobody abused: a false positive
                        elif kind[k] == "hijack":
                            detect.append((d["ns"] - attacks[k][0]) / 1e6)
                        for t, served, usd in recent.pop(k, ()):
                            account(k, t, served, usd)
        return run

    def start_attack(k, at, why):
        kind[k] = why
        attacks[k] = (at, at + int(a.attack_for * 1e9),
                      [f"AS{random.randrange(1000, 60000)}" for _ in range(a.attack_networks)])

    async def outside_world():
        # Hijacks and leaks on their own schedules, from their due times so a slow tick doesn't drift them.
        t0 = time.monotonic()
        next_h, next_l = t0 + a.first_at, t0 + a.first_at + a.leak_every / 2
        while True:
            now = time.monotonic()
            free = [k for k in random.sample(keys, 20) if k not in kind]
            if now >= next_h and free:
                start_attack(free[0], time.time_ns(), "hijack")
                stats["hijacks"] += 1
                next_h += a.hijack_every
            if now >= next_l and len(free) > 1:
                k = free[1]
                kind[k] = "leak"
                reports.append(SendMessage(msg(k=k, where=random.choice(("public repository", "app bundle")))))
                start_attack(k, time.time_ns() + int(a.attacker_after * 1e9), "leak")
                stats["leaks"] += 1
                next_l += a.leak_every
            await asyncio.sleep(0.05)

    async def send_reports():
        async with scanner:
            while True:
                await asyncio.sleep(0.002)
                if reports:
                    batch = reports[:]
                    reports.clear()
                    await scanner.send(batch)

    def attempt(s, k, net, usd, batches, abused):
        t = time.time_ns()
        served = k not in view[s]
        if abused:
            account(k, t, served, usd)
        if not served:
            stats["refused"] += 1           # 401: the key is disabled on this router
            return
        seq[(s, k)] += 1
        batches[pid[k]].append(SendMessage(msg(s=s, k=k, p=proj[k], net=net, seq=seq[(s, k)], usd=round(usd, 5),
                                               ns=t)))
        stats["served"] += 1
        stats["usd"] += usd

    async def request_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        attack_owed = defaultdict(float)
        async with producer:
            while True:
                t = time.monotonic() - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.requests_per_s * x / 100
                batches = defaultdict(list)
                for _ in range(int(owed)):
                    k = random.choice(heavy) if random.random() < 0.7 else random.choice(keys)
                    attempt(random.choice(routers), k, random.choice(nets[k]),
                            random.uniform(COST_LOW, COST_HIGH), batches, k in kind)
                owed -= int(owed)
                now_ns = time.time_ns()
                for k, (st, end, anets) in list(attacks.items()):
                    if now_ns >= end:
                        del attacks[k]
                        attack_owed.pop(k, None)
                        continue
                    if now_ns < st:
                        continue
                    attack_owed[k] += a.attack_rps / 100
                    for _ in range(int(attack_owed[k])):
                        attempt(random.choice(routers), k, random.choice(anets),
                                random.uniform(ABUSE_LOW, ABUSE_HIGH), batches, True)
                        stats["abusive"] += 1
                    attack_owed[k] -= int(attack_owed[k])
                # One batch per partition, all sent at once: per-key order is kept, about one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        print(f"[routers] {a.routers} request routers, {a.keys:,} API keys, a hijack every {a.hijack_every:g} s "
              f"and a leak every {a.leak_every:g} s at {a.attack_rps:g} requests/s each", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            win = f"{statistics.median(detect):.0f}ms" if detect else "-"
            print(f"[routers] {int(stats['served']):>7,} requests/s ({phase})  ${stats['usd']:,.2f}/s  "
                  f"abusive attempts {int(stats['abusive']):,}  refused {int(stats['refused']):,}  "
                  f"disables {int(stats['disables'])} (hijack detected in {win} median)  "
                  f"served after the disable {int(stats['after']):,} (${stats['after_usd']:.2f}; "
                  f"at {a.compare_ms:g} ms it would be {int(stats['would']):,}, ${stats['would_usd']:.2f})  "
                  f"false disables {int(stats['unplanted'])}  "
                  f"disable-to-routers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("served", "usd", "abusive", "refused", "disables", "after", "after_usd", "would", "would_usd",
                      "unplanted"):
                stats[k] = 0
            del detect[:-200]

    readers = [reader(s)() for s in routers]
    gc.freeze()                             # keep start-up objects out of the collector's way
    await asyncio.gather(request_loop(), report(), outside_world(), send_reports(), *readers)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--routers", type=int, default=8, help="request routers, each with its own key-status reader")
    p.add_argument("--keys", type=int, default=50_000, help="active API keys")
    p.add_argument("--requests-per-s", type=float, default=10_000, help="the keys' own requests per second, all routers")
    p.add_argument("--hijack-every", type=float, default=2, help="seconds between stolen keys put to use")
    p.add_argument("--leak-every", type=float, default=3, help="seconds between leaked keys found by secret scanning")
    p.add_argument("--attacker-after", type=float, default=0.2, help="seconds from a leak report to its first abuse")
    p.add_argument("--attack-rps", type=float, default=200, help="abusive requests per second on one stolen key")
    p.add_argument("--attack-for", type=float, default=6, help="seconds an attacker keeps trying one key")
    p.add_argument("--attack-networks", type=int, default=40, help="networks an attacker spreads one key over")
    p.add_argument("--first-at", type=float, default=5, help="seconds before the first hijack")
    p.add_argument("--compare-ms", type=float, default=1000, help="also count what a lane this far behind would serve")
    p.add_argument("--poll-ms", type=float, default=1, help="key-status poll wait when nothing is waiting")
    p.add_argument("--peak", type=float, default=1, help="request-rate multiplier during the peak (a model launch)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
