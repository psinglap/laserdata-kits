"""The anomaly check: a health score per network-function instance, and an alert the moment it falls.

Reads `vprobe-sdr` (the lane in) directly, partition by partition, on --readers connections of its own (no
consumer group), keeps the last --window records of every NF instance, and every --every-ms scores it:
procedure failure rate and procedure duration, each mapped to a score by a linear piecewise function,
weighted towards failures, smoothed by an EWMA, and put in severity buckets (normal, minor, major,
critical). At major or worse the instance is flagged degraded; back at normal it is healthy again. Each
change goes out at once on `nf-health`, keyed by instance, with a per-instance sequence and the resulting
state (never only "something changed"); every second the state of every instance is sent again, so a
reader that joins late converges.

It stands in for the real-time detection step of ATOM, which Jio describes as detecting anomalous network
patterns "down to individual end-nodes". The thresholds and weights here are illustrative; in production
the models stay yours. The kit's point is the lane around them: records in and alerts out, in milliseconds.

Prints SDR-to-check p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (ALERTS, DEFAULT_URL, PARTITIONS, SDRS, STREAM, Window, ensure_topics, load, msg, part,
                    read_partitions)

BUCKETS = [(0.80, "normal"), (0.60, "minor"), (0.40, "major"), (-1.0, "critical")]


def piecewise(x, good, bad):
    """1.0 at or better than `good`, 0.0 at or worse than `bad`, linear in between."""
    return max(0.0, min(1.0, (bad - x) / (bad - good)))


def bucket(score):
    return next(name for floor, name in BUCKETS if score >= floor)


async def main(a):
    async def own_client():                 # one connection per reader, one for the writer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SDRS, ALERTS)
    alerts = await (await own_client()).producer(
        STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS],
        mode=DirectProducerConfig(batch_length=500, linger_time=timedelta(microseconds=200)))
    window = defaultdict(lambda: deque(maxlen=a.window))   # NF instance -> last N (failed, duration ms)
    last_seq = defaultdict(int)
    onset = {}                              # NF instance -> start of the injected incident, for scoring only
    ewma, state, sig_seq = {}, {}, defaultdict(int)        # NF -> smoothed score; (status, severity, since ns)
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays earlier records
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_record(m):
        r = load(m.payload())
        k = r["k"]
        if r["seq"] <= last_seq[k]:
            stats["dropped"] += 1           # replayed or repeated record: counted once
            return
        last_seq[k] = r["seq"]
        window[k].append((r["res"] == "fail", r["dur"]))
        if r["deg"]:
            onset[k] = r["deg"]
        age = (time.time_ns() - r["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    def score(w):
        n = len(w)
        fr = sum(f for f, _ in w) / n
        dur = sum(d for _, d in w) / n
        return 0.7 * piecewise(fr, 0.01, 0.15) + 0.3 * piecewise(dur, 60, 400), fr

    def alert(k, now, fr):
        st, sev, since = state[k]
        sig_seq[k] += 1
        return k, SendMessage(msg(k=k, seq=sig_seq[k], status=st, sev=sev, score=round(ewma[k], 3), fail=round(fr, 3),
                                  since=since, onset=onset.get(k, 0) if st == "degraded" else 0, ns=now))

    async def publish(batch):
        groups = defaultdict(list)
        for k, m in batch:
            groups[part(k, PARTITIONS[ALERTS])].append(m)
        await asyncio.gather(*(alerts.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def check():
        last_refresh = time.monotonic()
        due = time.monotonic()
        async with alerts:
            while True:
                due += a.every_ms / 1000    # from the due time, so a slow round doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))
                if not caught_up[0]:
                    continue                # never alert from history
                now, out, frs = time.time_ns(), [], {}
                for k, w in window.items():
                    if len(w) < a.window:
                        continue
                    raw, frs[k] = score(w)
                    ewma[k] = a.alpha * raw + (1 - a.alpha) * ewma.get(k, raw)
                    sev = bucket(ewma[k])
                    st = state.get(k, ("healthy", sev, 0))[0]
                    if st != "degraded" and sev in ("major", "critical"):
                        state[k] = ("degraded", sev, now)
                        out.append(alert(k, now, frs[k]))
                        print(f"[{a.name}] degraded {k}  {sev}  score {ewma[k]:.2f}  failed {frs[k]:.0%} of last "
                              f"{a.window}", flush=True)
                    elif st == "degraded" and sev == "normal":
                        state[k] = ("healthy", sev, now)
                        out.append(alert(k, now, frs[k]))
                        onset.pop(k, None)
                        print(f"[{a.name}] healthy  {k}  {sev}  score {ewma[k]:.2f}", flush=True)
                    else:
                        state[k] = (st, sev, state.get(k, (0, 0, now))[2])
                if time.monotonic() - last_refresh >= 1:
                    last_refresh = time.monotonic()
                    sent = {o[0] for o in out}
                    out += [alert(k, now, frs.get(k, 0)) for k in state if k not in sent]
                if out:
                    await publish(out)
                    stats["alerts"] += len(out)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of records arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier records this second"
                      if stats["replayed"] else f"[{a.name}] waiting for the first records", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            down = sum(1 for s in state.values() if s[0] == "degraded")
            print(f"[{a.name}] {r['n']:>7,} SDRs/s  SDR-to-check p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  NFs {len(state)}  degraded {down}  "
                  f"alerts sent {stats['alerts']}", flush=True)
            stats["alerts"] = 0

    parts = list(range((await (await own_client()).get_topic(STREAM, SDRS)).partitions_count))
    readers = [read_partitions(await own_client(), SDRS, parts[i::a.readers], f"{a.name}-{os.getpid()}-{i}",
                               on_record, batch=a.batch) for i in range(a.readers)]
    await asyncio.gather(check(), report(), *readers)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="anomaly")
    p.add_argument("--readers", type=int, default=2, help="connections reading the lane's partitions")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--window", type=int, default=40, help="score the last N records of each NF instance")
    p.add_argument("--alpha", type=float, default=0.6, help="EWMA smoothing factor")
    p.add_argument("--every-ms", type=int, default=100, help="how often the score runs")
    p.add_argument("--budget-ms", type=int, default=100, help="SDR-to-check p99 budget for the lane")
    asyncio.run(main(p.parse_args()))
