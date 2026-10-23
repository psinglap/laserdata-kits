"""Mobile apps reporting through the ingest worker, two crash spikes, and the Slack and email senders that read every
spike alert.

Plays --apps apps (a few large, many small) uploading --batches ingest batches a second. Each batch comes from one
session of one app; after the ingest worker has grouped it (step 7 of its pipeline, "Bucket Exceptions & ANRs"),
one small record per batch goes to issue-occurrences, keyed by app ID: the batch ID, the app, the session, and the
fatal crash and ANR occurrences per issue fingerprint, usually none. Background crashes and ANRs are spread thinly
over --issues issues per app, so none of them should alert. A share of records (--dup-share) is delivered twice,
as an at-least-once bus does; so is every 10th record of a spike.

Two spikes, both on the lane (illustrative issues; the rule is Measure's):
  * A, a crash spike: from --spike-a-at s, --spike-a-rate crashes a second of one new issue in the largest app,
    for --spike-a-for s. It starts just after an hourly run.
  * B, an ANR spike: --spike-b-count ANRs of one issue in the second-largest app over --spike-b-for s, centred on
    an hourly run, so each clock hour sees only half of it.

Ground truth, from this process's own record of every distinct batch (common.SpikeWindow, the same rule):
  * live: which batch tips each issue over both thresholds, for the spike check to match exactly;
  * hourly: what Measure's hourly job would alert at the top of each hour (--check-every-s), over the previous
    --window-s, with its own cooldowns. --check-phase-s puts the first run that many seconds after start (for a
    short run); by default runs fall on the clock's hours, as cron "0 * * * *" does.

--senders readers play the Slack and email senders, each on its own connection, reading spike-alerts directly from
its own position. The business numbers: when Slack had each spike, from the spike's start; whether the alert
tipped on the right batch; when the hourly job would have had it, or that it never would; and the spike's crashes
that came after Slack had the alert but before the hourly job would have.
"""
import argparse
import asyncio
import gc
import hashlib
import heapq
import itertools
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ALERTS, COOLDOWN_S, ISSUES, MIN_COUNT, MIN_RATE, PARTITIONS, STREAM, WINDOW_S, DEFAULT_URL,
                    SpikeWindow, Stalls, Window, app_id, ensure_topics, load, msg, part)

SENDERS = ["slack", "email"]


def fingerprint(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()[:32]


SPIKE_A = ("crash", "java.lang.IllegalStateException @ CheckoutFragment.onResume")      # illustrative
SPIKE_B = ("anr", "main thread blocked @ SQLiteDatabase.query <- CartRepository.load")  # illustrative


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ISSUES, ALERTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, ISSUES, topic_partitions_count=PARTITIONS[ISSUES],
                                                   mode=cfg)
    start = time.time()
    start_ns = int(start * 1e9)
    weights = [1 / (i + 1) ** 1.1 for i in range(a.apps)]
    tot = sum(weights)
    weights = [w / tot for w in weights]
    apps = [app_id(i) for i in range(a.apps)]
    pools = [max(20, int(w * a.sessions)) for w in weights]      # concurrent sessions per app
    fa, fb = fingerprint(SPIKE_A[1]), fingerprint(SPIKE_B[1])
    spike = {(apps[0], "crash", fa): "A", (apps[1], "anr", fb): "B"}
    label = {"A": f"crash spike, {SPIKE_A[1]}", "B": f"ANR spike, {SPIKE_B[1]}"}
    period = a.check_every_s
    anchor = start + a.check_phase_s if a.check_phase_s is not None else 0.0
    nxt_check = anchor + period * (int((start - anchor) // period) + 1)
    b_mid = anchor + period * (int((start + a.spike_b_after - anchor) // period) + 1)
    began = {"A": start + a.spike_a_at, "B": b_mid - a.spike_b_for / 2}
    print(f"[ingest-worker] {a.apps} apps, {a.batches:g} batches/s, {a.sessions:,} concurrent sessions; spike rule "
          f">= {a.min_count} in {a.window_s:g} s and >= {a.min_rate:g}% of sessions; hourly job every {period:g} s, "
          f"next at +{nxt_check - start:.1f} s; spike A ({label['A']}) at +{began['A'] - start:.1f} s for "
          f"{a.spike_a_for:g} s at {a.spike_a_rate:g}/s; spike B ({label['B']}) {a.spike_b_count} ANRs from "
          f"+{began['B'] - start:.1f} s for {a.spike_b_for:g} s, around the run at +{b_mid - start:.1f} s",
          flush=True)

    truth_w = defaultdict(lambda: SpikeWindow(a.window_s))      # every distinct batch, as the spike check sees it
    live_cool, hourly_cool = {}, {}
    cool_ns = int(a.cooldown_s * 1e9)
    truth = {}                              # (app, kind, fp) -> (tip batch, tip ns): what the live check must send
    hourly = {}                             # (app, kind, fp) -> run time: what the hourly job would send
    rx = {}                                 # (app, kind, fp) -> when Slack had the alert
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    after_live = defaultdict(int)           # spike occurrences after Slack had it, before the hourly job would
    seen_alerts = set()
    bseq = itertools.count()

    def reader(name):
        def on_alert(m):
            d = load(m.payload())
            now = time.time_ns()
            key = (d["app"], d["kind"], d["fp"])
            lat.add((now - d["tip_ns"]) / 1e6)  # tipping batch to this sender, both hops
            if (name, key) in seen_alerts:
                return                      # repeated alert (at least once): already posted
            seen_alerts.add((name, key))
            stats[name] += 1
            if name != "slack":
                return
            rx[key] = now
            t = truth.get(key)
            s = spike.get(key)
            if s is None:
                stats["unexpected"] += 1
            match = "yes" if t and t[0] == d["tip_b"] else "no"
            stats["mismatch"] += match == "no"
            what = f"spike {s} ({label[s]})" if s else f"{d['title']} on app {d['app'][:8]} issue {d['fp'][:8]}"
            since = f"{now / 1e9 - began[s]:.1f} s after the spike began; " if s else ""
            print(f"[slack] ALERT {d['title']}: {what}: at Slack {(now - d['tip_ns']) / 1e6:.1f} ms after the tipping "
                  f"batch; {since}tip batch matches the truth: {match}; {d['count']} occurrences, "
                  f"{d['sessions']:,} sessions, {d['rate']:.2f}%", flush=True)
        return on_alert

    async def consume(name):
        # One connection per sender, reading the alerts partition directly from its own position (no consumer group,
        # nothing stored on the broker per poll), from this run's start.
        c, cb = await own_client(), reader(name)
        who, nxt = Consumer.Single(f"{name}-{os.getpid()}"), None
        since = PollingStrategy.Timestamp(int(start * 1e6))
        while True:
            ms = await c.poll_messages(STREAM, ALERTS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else since)
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    cb(m)
            if len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)

    def hourly_run(t):
        # Measure's hourly job at time t: for every app, every issue's occurrences in [t - window, t] against that
        # window's distinct sessions, each alerted issue then quiet for the cooldown. Same records, on the clock.
        t_ns = int(t * 1e9)
        fired = []
        for app, w in truth_w.items():
            w.expire(t_ns)
            for k in list(w.count):
                ck = (app, *k)
                last = hourly_cool.get(ck)
                if (last is None or t_ns - last >= cool_ns) and w.spiking(k, a.min_count, a.min_rate):
                    hourly_cool[ck] = t_ns
                    hourly.setdefault(ck, t)
                    fired.append(spike.get(ck) or f"{k[0]} {app[:8]}/{k[1][:8]}")
        counts = {s: truth_w[ck[0]].count.get(ck[1:], 0) for ck, s in spike.items()}
        print(f"[hourly] run at +{t - start:.1f} s: alerts {fired or 'none'}; in its window: spike A "
              f"{counts['A']} crashes, spike B {counts['B']} ANRs", flush=True)

    def record(now_ns, ai, session, x):
        app = apps[ai]
        b = f"{os.getpid():x}-{next(bseq):x}"
        w = truth_w[app]
        w.add(now_ns, session, x)
        if x:
            w.expire(now_ns, [(k, f) for k, f, _ in x])
            for kind, fp, n in x:
                ck = (app, kind, fp)
                hit = w.spiking((kind, fp), a.min_count, a.min_rate)
                last = live_cool.get(ck)
                if hit and (last is None or now_ns - last >= cool_ns):
                    live_cool[ck] = now_ns
                    truth.setdefault(ck, (b, now_ns))
                    s = spike.get(ck)
                    if s:
                        print(f"[ingest-worker] TRUTH spike {s} tips at batch {b}, {now_ns / 1e9 - began[s]:.1f} s "
                              f"after it began: {hit[0]} occurrences, {hit[1]:,} sessions, {hit[2]:.2f}%", flush=True)
                s = spike.get(ck)
                if s and ck in rx and ck not in hourly:
                    after_live[s] += n
        return app, b

    def session_of(ai, now):
        i = random.randrange(pools[ai])
        gen = int((now + i * 0.37) // a.session_s)        # each session lasts --session-s, staggered
        return f"{ai:x}.{i:x}.{gen:x}"

    async def apps_loop():
        nonlocal nxt_check
        due = []                            # (send at, tiebreak, partition, payload): repeats
        seqno = itertools.count()
        tick = time.monotonic()
        acc = acc_a = acc_b = 0.0
        b_rate = a.spike_b_count / a.spike_b_for
        async with producer:
            while True:
                now = time.time()
                if now >= nxt_check:        # the hourly job runs before this tick's batches
                    hourly_run(nxt_check)
                    nxt_check += period
                now_ns = time.time_ns()
                mono = time.monotonic()
                batches = defaultdict(list)

                def emit(ai, x, repeat_p):
                    sess = session_of(ai, now)
                    app, b = record(now_ns, ai, sess, x)
                    for kind, _, n in x:
                        stats["occ_" + kind] += n
                    sm = msg(b=b, a=app, s=sess, x=x, ns=now_ns)
                    pid = part(app, PARTITIONS[ISSUES])
                    batches[pid].append(SendMessage(sm))
                    stats["batches"] += 1
                    if random.random() < repeat_p:      # delivered again a little later (at least once)
                        heapq.heappush(due, (mono + random.uniform(0.05, 1.0), next(seqno), pid, sm))

                acc += a.batches * 0.01
                while acc >= 1:
                    acc -= 1
                    ai = random.choices(range(a.apps), weights)[0]
                    x = []
                    r = random.random()
                    if r < a.crash_p:
                        x.append(["crash", fingerprint(f"bg-{ai}-{random.randrange(a.issues)}"), 1])
                    elif r < a.crash_p + a.anr_p:
                        x.append(["anr", fingerprint(f"bg-anr-{ai}-{random.randrange(a.issues)}"), 1])
                    emit(ai, x, a.dup_share)
                if began["A"] <= now < began["A"] + a.spike_a_for:
                    acc_a += a.spike_a_rate * 0.01
                    while acc_a >= 1:       # one crashing device uploading its batch
                        acc_a -= 1
                        emit(0, [["crash", fa, 1]], 0.1)
                if began["B"] <= now < began["B"] + a.spike_b_for:
                    acc_b += b_rate * 0.01
                    while acc_b >= 1:
                        acc_b -= 1
                        emit(1, [["anr", fb, 1]], 0.1)
                while due and due[0][0] <= mono:
                    _, _, pid, sm = heapq.heappop(due)
                    batches[pid].append(SendMessage(sm))
                    stats["dups"] += 1
                # One batch per partition, all sent at once: per-app order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                tick += 0.01                # from its due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        total = defaultdict(int)
        while True:
            await asyncio.sleep(1)
            for k in ("slack", "email", "unexpected", "mismatch", "dups"):
                total[k] += int(stats[k])
            r = lat.flush()
            parts = []
            for ck, s in spike.items():
                live = f"{(rx[ck] / 1e9 - began[s]):.1f} s" if ck in rx else "not yet"
                hj = f"+{hourly[ck] - began[s]:.1f} s" if ck in hourly else "not yet"
                parts.append(f"spike {s}: Slack {live}, hourly job {hj}, after Slack before the hourly job "
                             f"{after_live[s]}")
            others = sum(1 for ck in hourly if ck not in spike)
            print(f"[ingest-worker] {int(stats['batches']):>6,} batches/s  occurrences crash {int(stats['occ_crash']):,} "
                  f"anr {int(stats['occ_anr']):,}  repeats sent {total['dups']:,}  {'; '.join(parts)}  alerts at Slack "
                  f"{total['slack']:,} email {total['email']:,} unexpected {total['unexpected']:,} tip mismatches "
                  f"{total['mismatch']:,}  hourly-job alerts for other issues {others}  tip-to-sender n {r['n']:,} "
                  f"p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  {stalls.read()}", flush=True)
            stats.clear()

    gc.freeze()                             # keep start-up objects out of the collector's way
    await asyncio.gather(apps_loop(), report(), stalls.watch(), *(consume(n) for n in SENDERS[: a.senders]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--apps", type=int, default=200, help="apps reporting")
    p.add_argument("--batches", type=float, default=1000, help="ingest batches per second, all apps")
    p.add_argument("--sessions", type=int, default=400_000, help="concurrent sessions, all apps")
    p.add_argument("--session-s", type=float, default=900, help="how long a session lasts")
    p.add_argument("--issues", type=int, default=400, help="background issues per app")
    p.add_argument("--crash-p", type=float, default=0.0005, help="share of batches with a background crash")
    p.add_argument("--anr-p", type=float, default=0.0003, help="share of batches with a background ANR")
    p.add_argument("--dup-share", type=float, default=0.01, help="share of records delivered twice")
    p.add_argument("--window-s", type=float, default=WINDOW_S, help="as the spike check: the window")
    p.add_argument("--min-count", type=int, default=MIN_COUNT, help="as the spike check")
    p.add_argument("--min-rate", type=float, default=MIN_RATE, help="as the spike check, %%")
    p.add_argument("--cooldown-s", type=float, default=COOLDOWN_S, help="as the spike check")
    p.add_argument("--check-every-s", type=float, default=3600, help="the hourly job's period")
    p.add_argument("--check-phase-s", type=float, default=None,
                   help="first hourly run this long after start (default: on the clock's hours)")
    p.add_argument("--spike-a-at", type=float, default=300, help="spike A starts, s after start")
    p.add_argument("--spike-a-for", type=float, default=900, help="s")
    p.add_argument("--spike-a-rate", type=float, default=3, help="crashes per second")
    p.add_argument("--spike-b-after", type=float, default=600,
                   help="spike B is centred on the first hourly run after this many s")
    p.add_argument("--spike-b-for", type=float, default=1200, help="s")
    p.add_argument("--spike-b-count", type=int, default=1800, help="ANRs in spike B")
    p.add_argument("--senders", type=int, default=2, help="alert senders (Slack, email), each with its own reader")
    p.add_argument("--poll-ms", type=float, default=1, help="sender wait between polls when nothing is waiting")
    asyncio.run(main(p.parse_args()))
