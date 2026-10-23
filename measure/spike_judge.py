"""The spike check, fed by the crash spike lane.

Reads every partition of issue-occurrences directly, on --readers connections of its own, tracking its own position
(no consumer group: a group reader visits one partition per poll, so a record can wait for its partition's turn).
Each record is one ingest batch after the ingest worker has grouped its crashes and ANRs into issues: the batch ID,
the app ID, the session, and the fatal crash and ANR occurrences per issue fingerprint (often none).

Per app, it keeps the spike rule's window (common.SpikeWindow): distinct sessions and occurrences per issue over the
last --window-s. On every record that carries an occurrence, it checks that issue against the app's thresholds
(--min-count, --min-rate; Measure's defaults are 100 and 0.5%) and, if both hold and the issue is not in its
cooldown (--cooldown-s, 7 days), raises a Crash Spike or ANR Spike alert at once. Records arrive at least once: a
batch ID seen in the window is skipped, as the ingest worker's checkSeen skips a batch it already ingested, so a
batch delivered twice never counts twice. The window runs on the records' own clock, so a replay gives the same
alerts at the same records.

On start it reads back --lookback-s of the log (the window, by default) to rebuild every app's hour before it
judges, and restores cooldowns from spike-alerts, so a restart neither misses a spike nor alerts an issue twice.
Alerts go to spike-alerts, which the Slack and email senders read; they are sent from their own task, never from
inside the read loop.

Prints batch-to-check p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the alerts raised.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (ALERTS, COOLDOWN_S, ISSUES, KINDS, MIN_COUNT, MIN_RATE, PARTITIONS, STREAM, WINDOW_S,
                    DEFAULT_URL, SpikeWindow, Stalls, Window, ensure_topics, load, msg)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ISSUES, ALERTS)
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    apps = defaultdict(lambda: SpikeWindow(a.window_s))
    seen = {}                               # batch ID -> record ns, for one window: checkSeen
    cooldown = {}                           # (app, kind, fingerprint) -> ns of its last alert
    cool_ns = int(a.cooldown_s * 1e9)
    pending = []                            # alerts waiting for the next send
    caught_up = [False]                     # a new reader first replays the window from the log

    # Cooldowns survive a restart: they are rebuilt from the alerts already sent.
    rc, nxt = await own_client(), None
    while True:
        ms = await rc.poll_messages(STREAM, ALERTS, consumer=Consumer.Single("spike-check-cooldowns"),
                                    partition_id=0, count=1000, auto_commit=False,
                                    polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                    else PollingStrategy.First())
        for m in ms:
            d = load(m.payload())
            cooldown[(d["app"], d["kind"], d["fp"])] = d["tip_ns"]
        if len(ms) < 1000:
            break
        nxt = ms[-1].offset() + 1
    restored = len(cooldown)
    out = await (await own_client()).producer(STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS])
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_record(m, now):
        d = load(m.payload())
        ns = d["ns"]
        age = (now - ns) / 1e6
        if caught_up[0]:
            lat.add(age)
            b = m.timestamp() / 1e3                 # broker append time, ms: where the time went
            stats["to_broker"] = max(stats["to_broker"], b - ns / 1e6)
            stats["from_broker"] = max(stats["from_broker"], now / 1e6 - b)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        stats["batches"] += 1
        if d["b"] in seen:
            stats["dupes"] += 1             # delivered twice (at least once): already counted
            return
        seen[d["b"]] = ns
        app, x = d["a"], d["x"]
        w = apps[app]
        w.add(ns, d["s"], x)
        if not x:
            return
        keys = [(kind, fp) for kind, fp, _ in x]
        w.expire(ns, keys)
        for kind, fp, n in x:
            stats["occ_" + kind] += n
            hit = w.spiking((kind, fp), a.min_count, a.min_rate)
            if hit is None:
                continue
            ck = (app, kind, fp)
            last = cooldown.get(ck)
            if last is not None and ns - last < cool_ns:
                stats["cooling"] += 1
                continue                    # this issue alerted in the last 7 days: stays quiet
            cooldown[ck] = ns
            count, sessions, rate = hit
            stats["fired_" + kind] += 1
            if not caught_up[0] and (now - ns) / 1e9 > a.replay_alert_s:
                continue                    # rebuilt from long ago: recorded for the cooldown, not sent again
            pending.append(SendMessage(msg(app=app, kind=kind, title=KINDS[kind], fp=fp, count=count,
                                           sessions=sessions, rate=round(rate, 3), tip_b=d["b"], tip_ns=ns)))

    async def read(pids):
        # Each partition in turn: first from the lookback point by timestamp, then on from our own position, with
        # nothing stored on the broker per poll. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-p{p}") for p in pids}
        since = PollingStrategy.Timestamp(int((time.time() - a.lookback_s) * 1e6))   # broker time, microseconds
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, ISSUES, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_record(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_alerts():
        # Alerts are small and rare; one batch every 2 ms keeps sends out of the read loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report():
        total = defaultdict(int)
        print(f"[spike-check] window {a.window_s:g} s, alert at >= {a.min_count} occurrences and >= {a.min_rate:g}% "
              f"of the window's sessions, cooldown {a.cooldown_s:g} s; {restored} cooldowns restored from "
              f"'{ALERTS}'", flush=True)
        while True:
            await asyncio.sleep(1)
            for k in ("dupes", "fired_crash", "fired_anr", "sent", "cooling"):
                total[k] += int(stats[k])
            if not caught_up[0]:
                # caught up once a whole second of records arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[spike-check] catching up: replayed {int(stats['replayed']):,} earlier batches this second, "
                      f"{len(apps):,} apps", flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[spike-check] {r['n']:>6,} batches/s  batch-to-check p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  occurrences crash {int(stats['occ_crash']):,} "
                  f"anr {int(stats['occ_anr']):,}  repeats skipped {int(stats['dupes']):,} ({total['dupes']:,} total)  "
                  f"alerts crash {total['fired_crash']:,} anr {total['fired_anr']:,} sent {total['sent']:,}  "
                  f"in cooldown {total['cooling']:,}  apps {len(apps):,}  {stalls.read()}  slowest batch->broker "
                  f"{stats['to_broker']:.0f} ms, broker->check {stats['from_broker']:.0f} ms", flush=True)
            stats.clear()
            # Once a second: drop what fell out of every app's window, and batch IDs older than the window.
            # Two seconds of slack, so the sweep never drops what a record still in flight would count.
            now = time.time_ns()
            for w in apps.values():
                w.expire(now - 2_000_000_000)
            cut = now - int(a.window_s * 1e9)
            for b in [b for b, t in seen.items() if t < cut]:
                del seen[b]

    parts = list(range(PARTITIONS[ISSUES]))
    await asyncio.gather(send_alerts(), report(), stalls.watch(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="spike-check")
    p.add_argument("--budget-ms", type=int, default=50, help="batch-to-check p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the issue-occurrences partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--window-s", type=float, default=WINDOW_S, help="the spike window (Measure: one hour)")
    p.add_argument("--min-count", type=int, default=MIN_COUNT, help="occurrences of one issue in the window")
    p.add_argument("--min-rate", type=float, default=MIN_RATE, help="those occurrences as %% of the window's sessions")
    p.add_argument("--cooldown-s", type=float, default=COOLDOWN_S, help="quiet period per issue after an alert")
    p.add_argument("--lookback-s", type=float, default=0, help="on start, read back this much (default: the window)")
    p.add_argument("--replay-alert-s", type=float, default=60,
                   help="after a restart, still send alerts whose tipping batch is younger than this")
    args = p.parse_args()
    args.lookback_s = args.lookback_s or args.window_s
    asyncio.run(main(args))
