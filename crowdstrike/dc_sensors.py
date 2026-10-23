"""Domain controller sensors: every authentication onto the lane, and every block applied before that account's
next authentication.

Plays --dcs domain controller sensors in one process. Each one checks every authentication against its own view
of enforced accounts and, if the account is still allowed, lets it through and publishes one record to
auth-events, keyed by account: DC, account, source host, destination host, protocol and a per-(DC, account)
sequence. Each DC also reads identity-enforcement directly on its own connection and applies every block or MFA
step to its own view. A DC never sees another's view, so an enforcement reaches every DC through the log.

The same process plays the outside world, on fixed schedules (all illustrative):
  * the accounts' own logons: --auths-per-s over --accounts accounts, a tenth of them (service accounts, busy
    users) carrying most of it, each from its own one or two workstations to its own handful of servers;
  * LATERAL movement every --lateral-every s: an attacker with a stolen credential authenticates from one
    foothold to host after host, --attack-rps a second, for --attack-for s (and keeps trying once cut off);
  * a COMPROMISED PASSWORD every --compromised-every s: an account event reports a password exposed; an attacker
    holding it starts using it --attacker-after s later.

The business numbers: authentications let through on an account after its block was published, because the block
had not reached that DC yet, and the new hosts the attacker reached that way; and the same for a lane
--compare-ms behind. Printed every second, with enforcement-to-DC p50/p99: the second hop of the loop.
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

from common import (ACCOUNT, AUTH, DEFAULT_URL, ENFORCE, PARTITIONS, PROTOCOLS, STREAM, Window, account,
                    ensure_topics, load, msg, own_hosts, part)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), AUTH, ACCOUNT, ENFORCE)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, AUTH, topic_partitions_count=PARTITIONS[AUTH], mode=cfg)
    reporter = await (await own_client()).producer(STREAM, ACCOUNT, topic_partitions_count=PARTITIONS[ACCOUNT])
    start_ns = time.time_ns()
    dcs = [f"dc-{i + 1:02d}" for i in range(a.dcs)]
    accts = [account(i) for i in range(a.accounts)]
    busy = accts[: max(1, a.accounts // 10)]
    pid = {x: part(x, PARTITIONS[AUTH]) for x in accts}
    hosts = {x: own_hosts(x) for x in accts}
    view = {s: {} for s in dcs}             # DC -> {account: enforcement ns}
    seq = defaultdict(int)                  # (DC, account) -> sequence
    attacks = {}                            # account -> (start, end, foothold): an attacker using the credential
    kind = {}                               # account -> "lateral" | "compromised", for accounts the outside world abused
    recent = defaultdict(lambda: deque(maxlen=2048))   # abused account -> (ns, allowed, dst) of recent attempts
    truth = {}                              # account -> enforcement ns, once the first DC has it
    reached, reached_late = defaultdict(set), defaultdict(set)   # account -> hosts reached after the block / 1 s lane
    detect = []                             # lateral: first attacker authentication to block published, ms
    reports = []                            # account events waiting for the reporter's send
    lat, stats = Window(), defaultdict(float)
    cmp_ns = a.compare_ms * 1_000_000

    def count(x, t, allowed, dst):
        """One attacker attempt on an abused account, counted against its block once that is known."""
        d = truth.get(x)
        if d is None:
            recent[x].append((t, allowed, dst))
            return
        if t <= d:
            return
        if t - d < cmp_ns:
            stats["would"] += 1             # a lane --compare-ms behind would have let it through
            if dst not in reached_late[x]:
                reached_late[x].add(dst)
                stats["would_hosts"] += 1
        if allowed:
            stats["after"] += 1             # let through after the block was published
            if dst not in reached[x]:
                reached[x].add(dst)
                stats["after_hosts"] += 1

    def reader(s):
        async def run():
            c = await own_client()
            who, nxt = Consumer.Single(f"{s}-{os.getpid()}"), None
            while True:
                ms = await c.poll_messages(STREAM, ENFORCE, consumer=who, partition_id=0, count=1000,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                           else PollingStrategy.First())
                if not ms:
                    await asyncio.sleep(a.poll_ms / 1000)
                    continue
                nxt = ms[-1].offset() + 1
                for m in ms:
                    d = load(m.payload())
                    x = d["a"]
                    if x in view[s]:
                        continue            # older or repeated enforcement: already applied
                    view[s][x] = d["ns"]
                    if d["ns"] <= start_ns:
                        continue            # replayed at start-up: applied, not timed
                    lat.add((time.time_ns() - d["ns"]) / 1e6)
                    if x not in truth:
                        truth[x] = d["ns"]
                        stats["enforced"] += 1
                        if x not in kind:
                            stats["unplanted"] += 1     # an account nobody abused was blocked: a false positive
                        elif kind[x] == "lateral":
                            detect.append((d["ns"] - attacks[x][0]) / 1e6)
                        for t, allowed, dst in recent.pop(x, ()):
                            count(x, t, allowed, dst)
        return run

    def start_attack(x, at, why):
        kind[x] = why
        attacks[x] = (at, at + int(a.attack_for * 1e9), f"ws-{random.randrange(90000):05d}")

    async def outside_world():
        # Lateral movement and compromised passwords on their own schedules, from their due times.
        t0 = time.monotonic()
        next_l, next_c = t0 + a.first_at, t0 + a.first_at + a.compromised_every / 2
        while True:
            now = time.monotonic()
            free = [x for x in random.sample(accts, 20) if x not in kind]
            if now >= next_l and free:
                start_attack(free[0], time.time_ns(), "lateral")
                stats["laterals"] += 1
                next_l += a.lateral_every
            if now >= next_c and len(free) > 1:
                x = free[1]
                kind[x] = "compromised"
                reports.append(SendMessage(msg(a=x, event="compromised_password",
                                               source=random.choice(("breach corpus", "password spray success")))))
                start_attack(x, time.time_ns() + int(a.attacker_after * 1e9), "compromised")
                stats["compromised"] += 1
                next_c += a.compromised_every
            await asyncio.sleep(0.05)

    async def send_reports():
        async with reporter:
            while True:
                await asyncio.sleep(0.002)
                if reports:
                    batch = reports[:]
                    reports.clear()
                    await reporter.send(batch)

    def attempt(s, x, src, dst, batches, abused):
        t = time.time_ns()
        allowed = x not in view[s]
        if abused:
            count(x, t, allowed, dst)
        if not allowed:
            stats["refused"] += 1           # blocked, or stopped at the MFA step, on this DC
            return
        seq[(s, x)] += 1
        batches[pid[x]].append(SendMessage(msg(s=s, a=x, src=src, dst=dst, proto=random.choice(PROTOCOLS),
                                               seq=seq[(s, x)], ns=t)))
        stats["auths"] += 1

    async def auth_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        attack_owed = defaultdict(float)
        async with producer:
            while True:
                t = time.monotonic() - t0
                k = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.auths_per_s * k / 100
                batches = defaultdict(list)
                for _ in range(int(owed)):
                    x = random.choice(busy) if random.random() < 0.7 else random.choice(accts)
                    src, dst = hosts[x]
                    attempt(random.choice(dcs), x, random.choice(src), random.choice(dst), batches, x in kind)
                owed -= int(owed)
                now_ns = time.time_ns()
                for x, (st, end, foothold) in list(attacks.items()):
                    if now_ns >= end:
                        del attacks[x]
                        attack_owed.pop(x, None)
                        continue
                    if now_ns < st:
                        continue
                    attack_owed[x] += a.attack_rps / 100
                    for _ in range(int(attack_owed[x])):
                        dst = f"ws-{random.randrange(90000):05d}" if random.random() < 0.7 \
                            else f"srv-{random.randrange(4000):04d}"
                        attempt(random.choice(dcs), x, foothold, dst, batches, True)
                        stats["attacker"] += 1
                    attack_owed[x] -= int(attack_owed[x])
                # One batch per partition, all sent at once: per-account order is kept, about one round trip a tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                stats["peak"] = k > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        print(f"[dcs] {a.dcs} domain controller sensors, {a.accounts:,} accounts, lateral movement every "
              f"{a.lateral_every:g} s and a compromised password every {a.compromised_every:g} s, "
              f"{a.attack_rps:g} attacker authentications/s each", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            win = f"{statistics.median(detect):.0f}ms" if detect else "-"
            print(f"[dcs] {int(stats['auths']):>7,} auths/s ({phase})  attacker attempts {int(stats['attacker']):,}  "
                  f"refused {int(stats['refused']):,}  blocks {int(stats['enforced'])} "
                  f"(lateral detected in {win} median)  "
                  f"let through after the block {int(stats['after']):,}, new hosts {int(stats['after_hosts']):,} "
                  f"(at {a.compare_ms:g} ms it would be {int(stats['would']):,}, new hosts "
                  f"{int(stats['would_hosts']):,})  false blocks {int(stats['unplanted'])}  "
                  f"enforcement-to-DCs n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("auths", "attacker", "refused", "enforced", "after", "after_hosts", "would", "would_hosts",
                      "unplanted"):
                stats[k] = 0
            del detect[:-200]

    readers = [reader(s)() for s in dcs]
    gc.freeze()                             # keep start-up objects out of the collector's way
    await asyncio.gather(auth_loop(), report(), outside_world(), send_reports(), *readers)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--dcs", type=int, default=8, help="domain controller sensors, each with its own enforcement reader")
    p.add_argument("--accounts", type=int, default=100_000, help="active accounts across customers")
    p.add_argument("--auths-per-s", type=float, default=10_000, help="the accounts' own authentications per second")
    p.add_argument("--lateral-every", type=float, default=2, help="seconds between stolen credentials used to move")
    p.add_argument("--compromised-every", type=float, default=3, help="seconds between compromised-password events")
    p.add_argument("--attacker-after", type=float, default=0.2, help="seconds from that event to the first attempt")
    p.add_argument("--attack-rps", type=float, default=100, help="attacker authentications per second, one account")
    p.add_argument("--attack-for", type=float, default=6, help="seconds an attacker keeps trying one account")
    p.add_argument("--first-at", type=float, default=5, help="seconds before the first lateral movement")
    p.add_argument("--compare-ms", type=float, default=1000, help="also count what a lane this far behind lets through")
    p.add_argument("--poll-ms", type=float, default=1, help="enforcement poll wait when nothing is waiting")
    p.add_argument("--peak", type=float, default=1, help="logon-rate multiplier during the peak (Monday 9 am)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
