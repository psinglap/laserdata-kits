"""The delivery alert check, fed by the outcomes lane.

Reads delivery-outcomes directly, partition by partition, on --readers connections of its own (no consumer group:
a group reader visits one partition per poll, so with 16 partitions a record waited for its partition's turn).
Each record is one delivery attempt to one destination of one workspace: workspace:destination, the HTTP status,
delivered or failed, the attempt number, and the delivery worker's sequence for that key, so a replayed or
repeated outcome is recognised and changes nothing.

For each workspace:destination it keeps the attempts of the last --window-s seconds and checks them against the
successful delivery rate that workspace set on its alert (common.threshold; decide() is where your rule goes):
  * ALERT once at least --min-failed attempts in the window failed and the delivered share fell below the threshold;
  * RESOLVED once at least --min-attempts are in the window and the failed share is under half the allowance.
Each change is published on delivery-alerts, which the alert service reads (delivery.py). An alert carries the
key and the resulting state (never "toggle"), and the newest alert per key wins, so a restarted check rebuilds
every key's state from the log.

Prints outcome-to-check p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the alerts.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import ALERT, ALERTS, DEFAULT_URL, OUTCOMES, PARTITIONS, RESOLVED, STREAM, Window, ensure_topics, load, msg, threshold


class KeyWindow:
    """The last --window-s seconds of delivery attempts for one workspace:destination, with running counts."""
    def __init__(self):
        self.q = deque()
        self.n = self.failed = 0

    def add(self, ns, ok, span):
        self.q.append((ns, ok))
        self.n += 1
        self.failed += not ok
        while self.q and self.q[0][0] < ns - span:
            _, o = self.q.popleft()
            self.n -= 1
            self.failed -= not o

    def clear(self):
        self.__init__()


def decide(current: str, w: KeyWindow, thr: float, a) -> str:
    """Stands in for your alert rule: the workspace's own successful-delivery threshold, over seconds."""
    allowance = 1.0 - thr                   # a 99% alert allows 1% of attempts to fail
    if current != ALERT:
        return ALERT if w.failed >= a.min_failed and w.failed > allowance * w.n else RESOLVED
    if w.n >= a.min_attempts and w.failed < allowance / 2 * w.n:
        return RESOLVED
    return ALERT


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), OUTCOMES, ALERTS)
    out = await (await own_client()).producer(STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS])
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (worker, key) -> last sequence applied
    window = defaultdict(KeyWindow)         # key -> recent attempts
    state = {}                              # key -> alert or resolved
    thr = {}                                # key -> the workspace's threshold
    pending = []                            # alerts waiting for the next send (one batch every 2 ms)
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
        w.add(d["ns"], d["ok"], span)
        t = thr.get(k)
        if t is None:
            t = thr[k] = threshold(k)
        cur = state.get(k, RESOLVED)
        new = decide(cur, w, t, a)
        if new == cur:
            return
        rate = 100.0 * (w.n - w.failed) / max(w.n, 1)
        state[k] = new
        w.clear()                           # judge the new state on fresh attempts only
        stats[new] += 1
        if age > a.replay_alert_s * 1000:
            return                          # replayed from long ago: state rebuilt, nothing to tell anyone
        pending.append(SendMessage(msg(k=k, state=new, thr=t, rate=round(rate, 1), status=d.get("status"))))
        if caught_up[0]:
            print(f"[{a.name}] {k} -> {new.upper()}  delivered {rate:.1f}% (threshold {t * 100:.0f}%, "
                  f"last status {d.get('status')})", flush=True)

    async def send_alerts():
        # Alerts are small and rare; one batch every 2 ms keeps them off a send-per-alert loop.
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
                      f"state rebuilt for {len(state):,} workspace destinations", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            n_open = sum(1 for v in state.values() if v == ALERT)
            print(f"[{a.name}] {r['n']:>7,} outcomes/s  outcome-to-check p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  failed {int(stats['failed']):,}  "
                  f"alerts sent {int(stats['sent']):,} (alert {int(stats[ALERT]):,}, resolved {int(stats[RESOLVED]):,})  "
                  f"open {n_open}", flush=True)
            stats.clear()

    parts = list(range((await (await own_client()).get_topic(STREAM, OUTCOMES)).partitions_count))
    await asyncio.gather(send_alerts(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the delivery-outcomes partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="alerts")
    p.add_argument("--budget-ms", type=int, default=100, help="outcome-to-check p99 budget for the lane")
    p.add_argument("--window-s", type=float, default=5.0, help="seconds of attempts judged per workspace:destination")
    p.add_argument("--min-failed", type=int, default=20, help="failed attempts needed in the window before alerting")
    p.add_argument("--min-attempts", type=int, default=50, help="attempts needed in the window before resolving")
    p.add_argument("--replay-alert-s", type=float, default=10,
                   help="after a restart, still publish alerts for outcomes younger than this")
    asyncio.run(main(p.parse_args()))
