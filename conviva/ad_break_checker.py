"""The ad break checker, fed by the live ad break lane.

Reads every partition of ad-events directly, on --readers connections of its own, tracking its own position and
storing it on the broker at most once a second (no consumer group: a group reader visits one partition per poll,
so an event can wait for its partition's turn). Each record is one ad event from one viewer session: break start,
ad attempt, impression (the creative started), ad start failure (response failure or creative media failure),
slate (a live server-side slot with no ad), break end. Every record carries the session's ad server, market and
device, the break and slot, the break's counts so far, and the session's event sequence, so a replayed or repeated
event is recognised and changes nothing.

The check (a stand-in for yours; put your own in Checker.decide()): for each cohort, ad server x market, the share
of unfilled slots over the last --window-s seconds of slot outcomes. With at least --min-slots outcomes, a share
at or above --fire-share raises an alert, and below --clear-share clears it. Each alert names the causes (response
failures, creative failures, slates) and the devices behind them. Alerts go to ad-alerts, which every consumer
reads (live_match.py plays the ad ops console, webhooks and agents); they are sent in small batches from their own
task, never from inside the read loop.

Prints event-to-checker p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the slot outcomes.
"""
import argparse
import asyncio
import gc
import time
from collections import Counter, defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (AD_EVENTS, ALERTS, CREATIVE_FAILURE, DEFAULT_URL, PARTITIONS, RESPONSE_FAILURE, SLATE, STREAM,
                    UNFILLED, Stalls, Window, ensure_topics, load, msg)


class Checker:
    """Per-cohort slot outcomes in one-second buckets, keyed by the events' own time, so a replay rebuilds the
    same windows."""

    def __init__(self, a):
        self.a = a
        self.buckets = defaultdict(dict)    # cohort -> {second: [slots, unfilled, Counter(cause), Counter(device)]}
        self.alerting = {}                  # cohort -> alert number of the open alert
        self.fired = Counter()              # cohort -> alerts raised so far

    def add(self, c, sec, outcome, dev):
        b = self.buckets[c]
        cell = b.get(sec)
        if cell is None:
            cell = b[sec] = [0, 0, Counter(), Counter()]
            for old in [s for s in b if s <= sec - self.a.window_s]:
                del b[old]
        cell[0] += 1
        if outcome in UNFILLED:
            cell[1] += 1
            cell[2][outcome] += 1
            cell[3][dev] += 1

    def decide(self, c, sec):
        """Returns ("fire" | "clear", details) or None."""
        n = unf = 0
        causes, devs = Counter(), Counter()
        for s, (k, u, cs, ds) in self.buckets[c].items():
            if s > sec - self.a.window_s:
                n, unf = n + k, unf + u
                causes.update(cs)
                devs.update(ds)
        if n < self.a.min_slots:
            return None
        share = unf / n
        if c not in self.alerting and share >= self.a.fire_share:
            self.fired[c] += 1
            self.alerting[c] = self.fired[c]
            return "fire", dict(share=round(share, 3), slots=n, causes=dict(causes),
                                devices=[d for d, _ in devs.most_common(3)])
        if c in self.alerting and share < self.a.clear_share:
            return "clear", dict(share=round(share, 3), slots=n)
        return None


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), AD_EVENTS, ALERTS)
    out = await (await own_client()).producer(STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS])
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    seq = {}                                # session -> last event sequence applied
    open_breaks = set()                     # sessions inside an ad break
    chk = Checker(a)
    pending = []                            # alerts waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays events from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_event(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
            b = m.timestamp() / 1e3                 # broker append time, ms: where the time went
            stats["to_broker"] = max(stats["to_broker"], b - d["ns"] / 1e6)
            stats["from_broker"] = max(stats["from_broker"], now / 1e6 - b)
        else:                               # still replaying: track the oldest event seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        s = d["s"]
        if d["seq"] <= seq.get(s, 0):
            stats["dropped"] += 1           # older or repeated event: already applied
            return
        seq[s] = d["seq"]
        stats["events"] += 1
        k = d["k"]
        if k == "break_start":
            open_breaks.add(s)
            return
        if k == "break_end":
            open_breaks.discard(s)
            return
        if k == "attempt":
            stats["attempts"] += 1
            return
        stats[k] += 1                       # impression or one of the unfilled outcomes
        c = (d["srv"], d["mkt"])
        sec = d["ns"] // 1_000_000_000
        chk.add(c, sec, k, d["dev"])
        r = chk.decide(c, sec)
        if r is None:
            return
        state, det = r
        n = chk.alerting[c]
        if state == "clear":
            del chk.alerting[c]
        stats[state] += 1
        if age > a.replay_alert_s * 1000:
            return                          # replayed from long ago: state rebuilt, nobody needs this alert now
        pending.append(SendMessage(msg(srv=c[0], mkt=c[1], state=state, alert=n, src_ns=d["ns"], **det)))

    async def read(pids):
        # Each partition in turn. Resume from the broker's stored offset once (from the start of the log the
        # first time), then read on from our own position, storing the offset at most once a second: an offset
        # write on every poll stalls the lane. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-p{p}") for p in pids}
        nxt, committed = {}, {}
        while True:
            more = False
            for p in pids:
                commit = time.monotonic() - committed.get(p, 0) >= a.commit_s
                ms = await c.poll_messages(STREAM, AD_EVENTS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=commit,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.Next())
                if ms:
                    if commit:
                        committed[p] = time.monotonic()
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_event(m, now)
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
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier events this second, "
                      f"state rebuilt for {len(seq):,} sessions", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            unf = int(stats[RESPONSE_FAILURE] + stats[CREATIVE_FAILURE] + stats[SLATE])
            alerting = ", ".join(f"{s} x {m}" for s, m in sorted(chk.alerting)) or "none"
            print(f"[{a.name}] {r['n']:>7,} events/s  event-to-checker p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  ad attempts {int(stats['attempts']):,}  "
                  f"impressions {int(stats['impression']):,}  unfilled {unf:,} (response failures "
                  f"{int(stats[RESPONSE_FAILURE]):,}, creative failures {int(stats[CREATIVE_FAILURE]):,}, slates "
                  f"{int(stats[SLATE]):,})  sessions in a break {len(open_breaks):,}  alerts sent "
                  f"{int(stats['sent']):,} (fire {int(stats['fire']):,}, clear {int(stats['clear']):,})  "
                  f"cohorts in alert: {alerting}  {stalls.read()}  slowest event->broker "
                  f"{stats['to_broker']:.0f} ms, broker->checker {stats['from_broker']:.0f} ms", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[AD_EVENTS]))
    await asyncio.gather(send_alerts(), report(), stalls.watch(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="ad-breaks")
    p.add_argument("--budget-ms", type=int, default=50, help="event-to-checker p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the ad-events partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--commit-s", type=float, default=1, help="store each partition's offset at most this often")
    p.add_argument("--window-s", type=int, default=10, help="seconds of slot outcomes per cohort")
    p.add_argument("--min-slots", type=int, default=40, help="slot outcomes needed before a cohort is judged")
    p.add_argument("--fire-share", type=float, default=0.25, help="unfilled share that raises a cohort alert")
    p.add_argument("--clear-share", type=float, default=0.10, help="unfilled share below which it clears")
    p.add_argument("--replay-alert-s", type=float, default=30,
                   help="after a restart, still send alerts for events younger than this")
    asyncio.run(main(p.parse_args()))
