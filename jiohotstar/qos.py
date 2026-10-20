"""The health check: a playback health score per cohort and CDN, and a signal the moment it falls.

Reads `heartbeats-lite` (the lane in) on its own connection and consumer group, keeps the last
--window heartbeats of every cohort|cdn pair, and every --every-ms scores it the way JioHotstar's
traffic-orchestration post (Mar 2026) describes the QoS Routing Manager: playback failure rate,
rebuffering and RTT mapped to scores by a linear piecewise function, a cumulative score weighted
towards failures, smoothed by an EWMA with alpha about 0.6, and put in severity buckets (Ideal,
Baseline, Sev3, Sev2, Sev1). At Sev2 or worse the pair is flagged degraded; back at Baseline or
better it is healthy again. Each change goes out at once on `cdn-health`, keyed by pair, with a
per-pair sequence and the resulting state (never only "something changed"); every second the state
of every pair is sent again, so a reader that joins late converges.

The thresholds and weights here are illustrative. In production the scoring stays yours; the kit's
point is the lane around it: heartbeats in and health signals out, in milliseconds.

Prints heartbeat-to-check p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, HEARTBEATS, PARTITIONS, SIGNALS, STREAM, Window, ensure_topics, load, msg, part

BUCKETS = [(0.90, "Ideal"), (0.75, "Baseline"), (0.55, "Sev3"), (0.35, "Sev2"), (-1.0, "Sev1")]


def piecewise(x, good, bad):
    """1.0 at or better than `good`, 0.0 at or worse than `bad`, linear in between."""
    return max(0.0, min(1.0, (bad - x) / (bad - good)))


def bucket(score):
    return next(name for floor, name in BUCKETS if score >= floor)


async def main(a):
    async def own_client():                 # one connection for the reader, one for the writer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), HEARTBEATS, SIGNALS)
    beats = await (await own_client()).consumer_group(
        f"{a.name}-{os.getpid()}", STREAM, HEARTBEATS, polling_strategy=PollingStrategy.First(), batch_length=5000,
        auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))
    signals = await (await own_client()).producer(
        STREAM, SIGNALS, topic_partitions_count=PARTITIONS[SIGNALS],
        mode=DirectProducerConfig(batch_length=500, linger_time=timedelta(microseconds=200)))
    window = defaultdict(lambda: deque(maxlen=a.window))   # pair -> last N (failed, buffering, rtt)
    last_seq = defaultdict(int)
    onset = {}                              # pair -> start of the injected incident, for scoring only
    ewma, state, sig_seq = {}, {}, defaultdict(int)        # pair -> smoothed score; (status, severity, since ns)
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays earlier heartbeats
    gc.freeze()

    async def on_heartbeat(m):
        h = load(m.payload())
        k = h["k"]
        if h["seq"] <= last_seq[k]:
            stats["dropped"] += 1           # replayed or repeated heartbeat: counted once
            return
        last_seq[k] = h["seq"]
        window[k].append((h["st"] == "failed", h["st"] == "buffering", h["rtt"]))
        if h["deg"]:
            onset[k] = h["deg"]
        age = (time.time_ns() - h["ns"]) / 1e6
        if not caught_up[0] and age < a.budget_ms:
            caught_up[0] = True             # replay is done; from here on, time live heartbeats
        if caught_up[0]:
            lat.add(age)
        else:
            stats["replayed"] += 1

    def score(w):
        n = len(w)
        pfr = sum(f for f, _, _ in w) / n
        rebuf = sum(b for _, b, _ in w) / n
        rtt = sum(r for _, _, r in w) / n
        s = 0.5 * piecewise(pfr, 0.01, 0.15) + 0.3 * piecewise(rebuf, 0.04, 0.30) + 0.2 * piecewise(rtt, 120, 600)
        return s, pfr

    def signal(k, now, pfr):
        st, sev, since = state[k]
        sig_seq[k] += 1
        return k, SendMessage(msg(k=k, seq=sig_seq[k], status=st, sev=sev, score=round(ewma[k], 3), pfr=round(pfr, 3),
                                  since=since, onset=onset.get(k, 0) if st == "degraded" else 0, ns=now))

    async def publish(batch):
        groups = defaultdict(list)
        for k, m in batch:
            groups[part(k, PARTITIONS[SIGNALS])].append(m)
        await asyncio.gather(*(signals.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def check():
        last_refresh = time.monotonic()
        async with signals:
            while True:
                await asyncio.sleep(a.every_ms / 1000)
                if not caught_up[0]:
                    continue                # never signal from history
                now, out, pfrs = time.time_ns(), [], {}
                for k, w in window.items():
                    if len(w) < a.window:
                        continue
                    raw, pfrs[k] = score(w)
                    ewma[k] = a.alpha * raw + (1 - a.alpha) * ewma.get(k, raw)
                    sev = bucket(ewma[k])
                    st = state.get(k, ("healthy", sev, 0))[0]
                    if st != "degraded" and sev in ("Sev2", "Sev1"):
                        state[k] = ("degraded", sev, now)
                        stats["degraded"] += 1
                        out.append(signal(k, now, pfrs[k]))
                        print(f"[qos] degraded {k}  {sev}  score {ewma[k]:.2f}  failed {pfrs[k]:.0%} of last {a.window}",
                              flush=True)
                    elif st == "degraded" and sev in ("Ideal", "Baseline"):
                        state[k] = ("healthy", sev, now)
                        out.append(signal(k, now, pfrs[k]))
                        onset.pop(k, None)
                        print(f"[qos] healthy  {k}  {sev}  score {ewma[k]:.2f}", flush=True)
                    else:
                        state[k] = (st, sev, state.get(k, (0, 0, now))[2])
                if time.monotonic() - last_refresh >= 1:
                    last_refresh = time.monotonic()
                    sent = {o[0] for o in out}
                    out += [signal(k, now, pfrs.get(k, 0)) for k in state if k not in sent]
                if out:
                    await publish(out)
                    stats["signals"] += len(out)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier heartbeats this second", flush=True)
                stats["replayed"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            down = sum(1 for s in state.values() if s[0] == "degraded")
            print(f"[{a.name}] {r['n']:>7,} heartbeats/s  heartbeat-to-check p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  pairs {len(state)}  degraded {down}  "
                  f"signals sent {stats['signals']}", flush=True)
            stats["signals"] = 0

    await asyncio.gather(beats.consume_messages(on_heartbeat, asyncio.Event()), check(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="qos")
    p.add_argument("--window", type=int, default=40, help="score the last N heartbeats of each cohort|cdn pair")
    p.add_argument("--alpha", type=float, default=0.6, help="EWMA smoothing factor")
    p.add_argument("--every-ms", type=int, default=100, help="how often the score runs")
    p.add_argument("--budget-ms", type=int, default=100, help="heartbeat-to-check p99 budget for the lane")
    asyncio.run(main(p.parse_args()))
