"""The release guard: the A/B comparison between the canary and the stable release, fed by the lane.

Reads every partition of payment-outcomes directly, on --readers connections of its own, tracking its own
position (no consumer group: a group reader visits one partition per poll, so with 16 partitions an outcome
waited for its partition's turn). It keeps success counts per release in 100 ms buckets, and every --tick seconds compares the canary with the stable release:
  * ABORT when, over the last --window seconds, the canary's success rate is lower by at least
    --min-drop with a z-score at or below -z_abort (two-proportion test), on at least --min-n canary payments;
  * SCALE to the next share on the ladder when the current stage has at least --stage-n canary
    payments over at least --stage-min-s seconds and the canary is not worse;
and publishes each decision on release-verdicts, which the rollout (router.py) applies.

Prints outcome-to-guard p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the
business numbers: canary versus stable success rate, the decision, and, on ABORT, the error code
that grew most and how long after the defect's first failed payment the decision was made.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import Counter, defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (DEFAULT_URL, LADDER, OUTCOMES, PARTITIONS, STREAM, SUCCESSES, VERDICTS, Window, ensure_topics,
                    load, msg, z_score)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), OUTCOMES, VERDICTS)
    verdicts = await (await own_client()).producer(STREAM, VERDICTS, topic_partitions_count=PARTITIONS[VERDICTS])
    lat, stats = Window(), defaultdict(int)
    seen = {}                               # payment ID -> (last attempt sequence, when)
    caught_up = [False]                     # a new reader first replays outcomes from before it joined
    g = {}                                  # the release under test in the live run
    gc.freeze()

    def reset(run, canary, stable):
        g.clear()
        g.update(run=run, canary=canary, stable=stable, share=LADDER[0], stage_at=time.monotonic(),
                 stage=[0, 0, 0, 0], buckets={}, seq=0, done=False, first_bad=0, last=None)

    def on_outcome(m):
        o = load(m.payload())
        now = time.time_ns()
        age = (now - o["ns"]) / 1e6
        if not caught_up[0]:                # still replaying: track the oldest outcome seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
            return
        lat.add(age)
        if age > 5000:
            return                          # history, not a payment happening now
        pid = o["pid"]
        if o["seq"] <= seen.get(pid, (0,))[0]:
            stats["dropped"] += 1           # older or repeated attempt: newest wins
            return
        seen[pid] = (o["seq"], now)
        if g.get("run") != o["run"]:
            if not o["canary"]:
                return                      # wait for the canary to name itself
            reset(o["run"], o["rel"], None)
        if o["canary"] and o["rel"] != g["canary"]:
            return
        if not o["canary"]:
            g["stable"] = o["rel"]
        ok = o["status"] in SUCCESSES
        b = g["buckets"].setdefault(o["ns"] // 100_000_000, [0, 0, 0, 0, Counter(), Counter()])
        i = 0 if o["canary"] else 2
        b[i] += ok
        b[i + 1] += 1
        g["stage"][i] += ok
        g["stage"][i + 1] += 1
        if not ok:
            b[4 if o["canary"] else 5][o["code"]] += 1
        if o.get("bad") and not g["first_bad"]:
            g["first_bad"] = o["ns"]        # ground truth from the kit's injected defect, to time the decision

    async def read(pids):
        # Each partition in turn, from the start of the log (the guard rebuilds its view from it), then from its
        # own position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, OUTCOMES, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_outcome(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def decide():
        while True:
            await asyncio.sleep(a.tick)
            if not g or g["done"]:
                continue
            now = time.time_ns()
            lo = (now - int(a.window * 1e9)) // 100_000_000
            for k in [k for k in g["buckets"] if k < lo]:
                del g["buckets"][k]
            ok_c = n_c = ok_s = n_s = 0
            codes_c, codes_s = Counter(), Counter()
            for b in g["buckets"].values():
                ok_c, n_c, ok_s, n_s = ok_c + b[0], n_c + b[1], ok_s + b[2], n_s + b[3]
                codes_c.update(b[4])
                codes_s.update(b[5])
            if not n_c or not n_s:
                continue
            p_c, p_s = ok_c / n_c, ok_s / n_s
            z = z_score(ok_c, n_c, ok_s, n_s)
            g["last"] = (p_c, n_c, p_s, n_s, z)
            sc = g["stage"]
            action, extra, span = None, "", f"{a.window:g} s"
            if n_c >= a.min_n and z <= -a.z_abort and p_s - p_c >= a.min_drop:
                action, g["share"], g["done"] = "ABORT", 0, True
                growth = {c: codes_c[c] / n_c - codes_s.get(c, 0) / n_s for c in codes_c}
                top = max(growth, key=growth.get)
                extra = f"  top error growth on canary: {top} +{100 * growth[top]:.1f} pts"
                if g["first_bad"]:
                    extra += f"; decided {(now - g['first_bad']) / 1e9:.2f} s after the defect's first failed payment"
            elif (sc[1] >= a.stage_n and time.monotonic() - g["stage_at"] >= a.stage_min_s
                  and z_score(sc[0], sc[1], sc[2], sc[3]) > -2):
                # The SCALE test looks at the whole stage, so report the stage's numbers with it.
                p_c, n_c, p_s, n_s = sc[0] / sc[1], sc[1], sc[2] / max(sc[3], 1), sc[3]
                z, span = z_score(*sc), f"the {g['share']}% stage"
                nxt = [s for s in LADDER if s > g["share"]]
                g["share"] = nxt[0] if nxt else 100
                action = "PROMOTE" if g["share"] >= 100 else "SCALE"
                g["done"] = action == "PROMOTE"
                g["stage"], g["stage_at"] = [0, 0, 0, 0], time.monotonic()
            if action:
                g["seq"] += 1
                await verdicts.send([SendMessage(msg(run=g["run"], seq=g["seq"], rel=g["canary"], action=action,
                                                     share=g["share"], p_c=p_c, p_s=p_s, z=z))])
                print(f"[verdict] {action} {g['canary']} -> {g['share']}%  success canary {100 * p_c:.1f}% (n {n_c:,}) "
                      f"vs stable {100 * p_s:.1f}% (n {n_s:,}) over {span}  z {z:+.1f}{extra}", flush=True)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of outcomes arrived inside the budget; partitions interleave,
                # so one fresh outcome alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier outcomes this second", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} outcomes/s  outcome-to-guard p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  payments tracked {len(seen):,}", flush=True)
            if g.get("last"):
                p_c, n_c, p_s, n_s, z = g["last"]
                state = "ABORTED" if g["done"] and g["share"] == 0 else ("PROMOTED" if g["done"] else "HOLD")
                print(f"[ab] canary {g['canary']} at {g['share']}%  success {100 * p_c:.1f}% (n {n_c:,}) vs stable "
                      f"{g['stable']} {100 * p_s:.1f}% (n {n_s:,}) over {a.window:g} s  z {z:+.1f}  {state}", flush=True)
            stats.clear()
            cut = time.time_ns() - 120 * 10 ** 9
            if len(seen) > 2_000_000:       # forget payments with no attempt in two minutes
                for pid in [p for p, (_, t) in seen.items() if t < cut]:
                    del seen[pid]

    async with verdicts:
        parts = list(range(PARTITIONS[OUTCOMES]))
        await asyncio.gather(decide(), report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="guard")
    p.add_argument("--budget-ms", type=int, default=50, help="outcome-to-guard p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the payment-outcomes partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--window", type=float, default=5, help="seconds of outcomes the ABORT test looks at")
    p.add_argument("--tick", type=float, default=0.2, help="seconds between decisions")
    p.add_argument("--min-n", type=int, default=100, help="canary payments needed in the window before an ABORT")
    p.add_argument("--min-drop", type=float, default=0.03, help="success-rate drop needed for an ABORT (0.03 = 3 pts)")
    p.add_argument("--z-abort", type=float, default=4.5, help="z-score at or below minus this aborts")
    p.add_argument("--stage-n", type=int, default=2_000, help="canary payments needed before the next SCALE")
    p.add_argument("--stage-min-s", type=float, default=60, help="seconds a stage lasts at least")
    asyncio.run(main(p.parse_args()))
