"""The provider health evaluator, fed by the outcomes lane.

Reads provider-outcomes directly, partition by partition, on --readers connections of its own
(no consumer group: a group reader visits one partition per poll, so with 16 partitions a record waited for its
partition's turn). Each record is one provider call in one turn:
stage:provider:region, ok or failed, time to first byte, and the worker's sequence for that key, so a replayed or
repeated outcome is recognised and changes nothing.

For each key it keeps the outcomes of the last --window-s seconds and decides (decide() is where your rule goes):
  * DEGRADED once at least --min-samples outcomes are in the window and --error-rate of them failed;
  * OK again once at least --min-probes probes are in the window and under --recover-rate of them failed.
Each change is published on provider-health, which every call worker reads (call_workers.py). A verdict carries
the key and the resulting state (never "toggle"), and the newest verdict per key wins, so a restarted evaluator
rebuilds every key's state from the log.

Prints outcome-to-evaluator p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the verdicts.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import DEFAULT_URL, DEGRADED, HEALTH, OK, OUTCOMES, PARTITIONS, STREAM, Window, ensure_topics, load, msg


class KeyWindow:
    """The last --window-s seconds of outcomes for one stage:provider:region, with running counts."""
    def __init__(self):
        self.q = deque()
        self.n = self.failed = self.probes = self.probes_failed = 0

    def _count(self, ok, probe, sign):
        self.n += sign
        self.failed += sign * (not ok)
        self.probes += sign * probe
        self.probes_failed += sign * (probe and not ok)

    def add(self, ns, ok, probe, span):
        self.q.append((ns, ok, probe))
        self._count(ok, probe, 1)
        while self.q and self.q[0][0] < ns - span:
            _, o, p = self.q.popleft()
            self._count(o, p, -1)

    def clear(self):
        self.__init__()


def decide(current: str, w: KeyWindow, a) -> str:
    """Stands in for your health rule (error rate here; add time to first byte, error codes, your thresholds)."""
    if current != DEGRADED:
        return DEGRADED if w.n >= a.min_samples and w.failed >= a.error_rate * w.n else OK
    if w.probes >= a.min_probes and w.probes_failed < a.recover_rate * w.probes:
        return OK
    return DEGRADED


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), OUTCOMES, HEALTH)
    out = await (await own_client()).producer(STREAM, HEALTH, topic_partitions_count=PARTITIONS[HEALTH])
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (worker, key) -> last sequence applied
    window = defaultdict(KeyWindow)         # key -> recent outcomes
    health = {}                             # key -> degraded or ok
    pending = []                            # verdicts waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays outcomes from before it joined
    span = int(a.window_s * 1e9)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_outcome(m):
        d = load(m.payload())
        now = time.time_ns()
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest outcome seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k, sk = d["k"], (d["w"], d["k"])
        if d["seq"] <= seq.get(sk, 0):
            stats["dropped"] += 1           # older or repeated outcome: already counted
            return
        seq[sk] = d["seq"]
        stats["outcomes"] += 1
        stats["failed"] += not d["ok"]
        w = window[k]
        w.add(d["ns"], d["ok"], d.get("probe", False), span)
        cur = health.get(k, OK)
        new = decide(cur, w, a)
        if new == cur:
            return
        health[k] = new
        w.clear()                           # judge the new state on fresh outcomes only
        stats[new] += 1
        if age > a.replay_verdict_s * 1000:
            return                          # replayed from long ago: state rebuilt, nothing to tell the workers
        pending.append(SendMessage(msg(k=k, state=new)))
        if caught_up[0]:
            print(f"[{a.name}] {k} -> {new.upper()}", flush=True)

    async def send_verdicts():
        # Verdicts are small and rare; one batch every 2 ms keeps them off a send-per-verdict loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, OUTCOMES, consumer=who, partition_id=p, count=a.batch,
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

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of outcomes arrived inside the budget; partitions interleave,
                # so one fresh outcome alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier outcomes this second, "
                      f"state rebuilt for {len(health):,} provider keys", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            down = sorted(k for k, v in health.items() if v == DEGRADED)
            print(f"[{a.name}] {r['n']:>7,} outcomes/s  outcome-to-evaluator p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  failed {int(stats['failed']):,}  "
                  f"verdicts sent {int(stats['sent']):,} (degraded {int(stats[DEGRADED]):,}, ok {int(stats[OK]):,})  "
                  f"degraded now {','.join(down) or '-'}", flush=True)
            stats.clear()

    parts = list(range((await (await own_client()).get_topic(STREAM, OUTCOMES)).partitions_count))
    await asyncio.gather(send_verdicts(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the provider-outcomes partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="health")
    p.add_argument("--budget-ms", type=int, default=50, help="outcome-to-evaluator p99 budget for the lane")
    p.add_argument("--window-s", type=float, default=1.0, help="seconds of outcomes judged per key")
    p.add_argument("--min-samples", type=int, default=20, help="outcomes needed before marking a key degraded")
    p.add_argument("--error-rate", type=float, default=0.25, help="failed share that marks a key degraded")
    p.add_argument("--min-probes", type=int, default=5, help="probes needed before marking a key ok again")
    p.add_argument("--recover-rate", type=float, default=0.1, help="failed share of probes under which it recovers")
    p.add_argument("--replay-verdict-s", type=float, default=10,
                   help="after a restart, still publish verdicts for outcomes younger than this")
    asyncio.run(main(p.parse_args()))
