"""The downtime detector: success rate per payment cohort, and a downtime signal the moment it falls.

Reads `payment-status` (the lane in) on its own connection, every partition read directly, keeps the last
--window payments of every cohort, and every --every-ms runs the rule the way Razorpay's 2021
post describes it: success rate of the last N payments against a threshold. Below --down-below
a downtime starts; it resolves once the rate is back above --up-above. Each change goes out at
once on `downtime-signals`, keyed by cohort, with a per-cohort sequence and the resulting state
(never only "something changed"); every second the current state of every cohort is sent again,
so a reader that joins late converges.

The rule here is deliberately simple. In production the detector stays yours (Flink today); the
kit's point is the lane around it: payment status in and downtime signals out, in milliseconds.

Prints status-to-detector p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, SIGNALS, STATUS, STREAM, Window, ensure_topics, load, msg, part, read_partitions


async def main(a):
    async def own_client():                 # one connection for the reader, one for the writer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), STATUS, SIGNALS)
    status = await own_client()             # direct partition reads, rebuilt from the first message
    signals = await (await own_client()).producer(
        STREAM, SIGNALS, topic_partitions_count=PARTITIONS[SIGNALS],
        mode=DirectProducerConfig(batch_length=500, linger_time=timedelta(microseconds=200)))
    window = defaultdict(lambda: deque(maxlen=a.window))   # cohort -> last N outcomes (1 captured, 0 failed)
    last_seq = defaultdict(int)
    onset = {}                              # cohort -> start of the injected incident, for scoring only
    state, sig_seq = {}, defaultdict(int)   # cohort -> (status, sr, begin ns)
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays earlier payments
    gc.freeze()

    async def on_status(m):
        p = load(m.payload())
        k = p["k"]
        if p["seq"] <= last_seq[k]:
            stats["dropped"] += 1           # replayed or repeated payment: counted once
            return
        last_seq[k] = p["seq"]
        window[k].append(p["status"] == "captured")
        if p["deg"]:
            onset[k] = p["deg"]
        age = (time.time_ns() - p["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest payment seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    def signal(k, now):
        st, sr, begin = state[k]
        sig_seq[k] += 1
        return k, SendMessage(msg(k=k, seq=sig_seq[k], status=st, severity="medium" if st == "started" else None,
                                  sr=round(sr, 3), begin=begin, onset=onset.get(k, 0) if st == "started" else 0, ns=now))

    async def publish(batch):
        groups = defaultdict(list)
        for k, m in batch:
            groups[part(k, PARTITIONS[SIGNALS])].append(m)
        await asyncio.gather(*(signals.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def detect():
        last_refresh = time.monotonic()
        async with signals:
            while True:
                await asyncio.sleep(a.every_ms / 1000)
                if not caught_up[0]:
                    continue                # never signal from history
                now, out = time.time_ns(), []
                for k, w in window.items():
                    if len(w) < a.window:
                        continue
                    sr = sum(w) / len(w)
                    st = state.get(k, ("up", 1.0, 0))[0]
                    if st != "started" and sr < a.down_below:
                        state[k] = ("started", sr, now)
                        stats["started"] += 1
                        out.append(signal(k, now))
                        print(f"[downtime] started  {k}  success rate of last {a.window} {sr:.0%}", flush=True)
                    elif st == "started" and sr >= a.up_above:
                        state[k] = ("resolved", sr, state[k][2])
                        stats["resolved"] += 1
                        out.append(signal(k, now))
                        onset.pop(k, None)
                        print(f"[downtime] resolved {k}  success rate of last {a.window} {sr:.0%}", flush=True)
                    else:
                        state[k] = (st if k in state else "up", sr, state.get(k, (0, 0, 0))[2])
                if time.monotonic() - last_refresh >= 1:
                    last_refresh = time.monotonic()
                    out += [signal(k, now) for k in state if not any(k == o[0] for o in out)]
                if out:
                    await publish(out)
                    stats["signals"] += len(out)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of payments arrived inside the budget; partitions interleave,
                # so one fresh payment alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier payments this second", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            down = sum(1 for s in state.values() if s[0] == "started")
            print(f"[{a.name}] {r['n']:>7,} payment events/s  status-to-detector p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  cohorts {len(state)}  down {down}  "
                  f"signals sent {stats['signals']}", flush=True)
            stats["signals"] = 0

    await asyncio.gather(read_partitions(status, STATUS, range(PARTITIONS[STATUS]), f"{a.name}-{os.getpid()}", on_status,
                                         replay=True, poll_ms=0.5), detect(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="downtime")
    p.add_argument("--window", type=int, default=40, help="success rate over the last N payments of a cohort")
    p.add_argument("--down-below", type=float, default=0.50, help="a downtime starts below this success rate")
    p.add_argument("--up-above", type=float, default=0.70, help="and resolves at or above this one")
    p.add_argument("--every-ms", type=int, default=100, help="how often the rule runs")
    p.add_argument("--budget-ms", type=int, default=100, help="status-to-detector p99 budget for the lane")
    asyncio.run(main(p.parse_args()))
