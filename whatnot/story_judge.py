"""The story judge, fed by the story SLO lane.

Reads every partition of story-events directly, on --readers connections of its own, tracking its own position
(no consumer group: a group reader visits one partition per poll, so an event can wait for its partition's turn).
On start it reads from --lookback-s before now (by the broker's timestamps), longer than a deadline, so after a
restart every story still open is rebuilt from the log before any deadline is judged. Each record is one event of one story: the trigger (a user taps a
livestream tile), a step (the server authorizes the join), an outcome (video playing, auction shown, chat visible)
or an expected-failure role (the seller blocked this user). Every record carries the story key, the story type,
the role, the client and its surface (ios, android, web).

Judging follows the rules in the story schema (common.STORIES):
  * a story succeeds the moment its last required outcome arrives;
  * it fails at its deadline (--deadline-s after the trigger) if any outcome is still missing; the judge waits
    --grace-ms past the deadline for events already in flight, then names the missing outcomes;
  * an expected-failure role ends it without counting it;
  * events arrive at least once: a repeated event, or any event after the story's verdict, changes nothing, so
    every story is judged once.
SLI (a stand-in for yours; put your own in Judge.sli()): for each story type x surface, over the last --window-s
seconds of verdicts, every client counts once with its worst result, and the SLI is the share of clients whose
stories succeeded. Below --fire-sli with at least --min-clients raises an alert; at or above --clear-sli clears it.
Alerts and a one-second SLI snapshot go to slo-alerts, which every consumer reads (live_shows.py plays the pager,
the Datadog exporter and the incident bot); they are sent from their own task, never from inside the read loop.

Prints event-to-judge p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, the stories judged and the
SLI per story type x surface.
"""
import argparse
import asyncio
import gc
import heapq
import time
from collections import Counter, defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (DEFAULT_URL, PARTITIONS, STORIES, STORY_EVENTS, STREAM, VERDICTS, Stalls, Window,
                    ensure_topics, load, msg)

ROLES = {}                                  # (story type, role) -> "trigger" | "step" | "outcome" | "expected"
for _st, _s in STORIES.items():
    ROLES[(_st, _s["trigger"])] = "trigger"
    ROLES.update({(_st, r): "step" for r in _s["steps"]})
    ROLES.update({(_st, r): "outcome" for r in _s["outcomes"]})
    ROLES.update({(_st, r): "expected" for r in _s["expected"]})


class Story:
    __slots__ = ("st", "sf", "c", "deadline", "got", "seen")

    def __init__(self, st, sf, c, deadline):
        self.st, self.sf, self.c, self.deadline = st, sf, c, deadline
        self.got = set()                    # required outcomes arrived in time
        self.seen = set()                   # roles applied, so a repeated event changes nothing


class Judge:
    """Verdicts per story type x surface in one-second buckets of verdict time; each bucket keeps every client's
    worst result, so a client that retries a hundred times still counts once."""

    def __init__(self, a):
        self.a = a
        self.buckets = defaultdict(dict)    # cohort -> {second: {client: 0 | 1}}
        self.missing = defaultdict(dict)    # cohort -> {second: Counter(missing outcome)}
        self.alerting = {}                  # cohort -> alert number of the open alert
        self.fired = Counter()
        self.dirty = set()
        self.tip = {}                       # cohort -> when the newest verdict became knowable (ns)
        self.tip_fail = {}                  # cohort -> deadline of the newest failed story (ns)

    def add(self, cohort, sec, client, ok, missing, due_ns):
        b = self.buckets[cohort]
        cell = b.get(sec)
        if cell is None:
            cell = b[sec] = {}
            self.missing[cohort][sec] = Counter()
            for old in [s for s in b if s <= sec - self.a.window_s]:
                del b[old]
                self.missing[cohort].pop(old, None)
        cell[client] = min(cell.get(client, 1), ok)
        if missing:
            self.missing[cohort][sec].update(missing)
            self.tip_fail[cohort] = max(self.tip_fail.get(cohort, 0), due_ns)
        self.dirty.add(cohort)
        self.tip[cohort] = max(self.tip.get(cohort, 0), due_ns)

    def sli(self, cohort, sec):
        worst = {}
        for s, cell in self.buckets[cohort].items():
            if s > sec - self.a.window_s:
                for c, ok in cell.items():
                    if ok < worst.get(c, 2):
                        worst[c] = ok
        n = len(worst)
        return (sum(worst.values()) / n if n else 1.0), n

    def decide(self, cohort, sec):
        """Returns ("fire" | "clear", details) or None."""
        sli, n = self.sli(cohort, sec)
        if n < self.a.min_clients:
            return None
        miss = Counter()
        for s, cnt in self.missing[cohort].items():
            if s > sec - self.a.window_s:
                miss.update(cnt)
        if cohort not in self.alerting and sli < self.a.fire_sli:
            self.fired[cohort] += 1
            self.alerting[cohort] = self.fired[cohort]
            return "fire", dict(sli=round(sli, 4), clients=n, missing=dict(miss.most_common(3)))
        if cohort in self.alerting and sli >= self.a.clear_sli:
            return "clear", dict(sli=round(sli, 4), clients=n)
        return None


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), STORY_EVENTS, VERDICTS)
    out = await (await own_client()).producer(STREAM, VERDICTS, topic_partitions_count=PARTITIONS[VERDICTS])
    lat, sweep_lag, stats, stalls = Window(), Window(), defaultdict(float), Stalls()
    deadline_ns = int(a.deadline_s * 1e9)
    grace_ns = int(a.grace_ms * 1e6)
    open_ = {}                              # story key -> Story, until its verdict
    closed = {}                             # story key -> (verdict time ns, ok), kept 2 deadlines to catch repeats
    due = []                                # (deadline ns, story key)
    judge = Judge(a)
    pending = []                            # alerts waiting for the next send
    caught_up = [False]                     # a new reader first replays events from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def verdict(key, s, ok, missing, due_ns, now):
        del open_[key]
        closed[key] = (now, ok)
        stats["ok" if ok else "failed"] += 1
        judge.add((s.st, s.sf), now // 1_000_000_000, s.c, 1 if ok else 0, missing, due_ns)

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
        stats["events"] += 1
        key, st, r = d["k"], d["st"], d["r"]
        role = ROLES.get((st, r))
        if role is None:
            stats["unknown"] += 1
            return
        done = closed.get(key)
        if done is not None:
            # an outcome for a story already failed at its deadline is late; anything else is a repeat
            stats["late" if role == "outcome" and not done[1] else "after"] += 1
            return                          # the story already has its verdict: judged once
        s = open_.get(key)
        if s is None:
            if role != "trigger":
                stats["orphans"] += 1       # an event whose trigger this judge never saw
                return
            s = open_[key] = Story(st, d["sf"], d["c"], d["ns"] + deadline_ns)
            heapq.heappush(due, (s.deadline, key))
        if r in s.seen:
            stats["dupes"] += 1             # delivered twice (at least once): already applied
            return
        s.seen.add(r)
        if role == "expected":
            del open_[key]
            closed[key] = (now, True)
            stats["excluded"] += 1          # a product rule ended it: not counted against the SLI
        elif role == "outcome":
            if d["ns"] > s.deadline:
                stats["late"] += 1
                return
            s.got.add(r)
            if len(s.got) == len(STORIES[st]["outcomes"]):
                verdict(key, s, True, None, d["ns"], now)

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
                ms = await c.poll_messages(STREAM, STORY_EVENTS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_event(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def tick():
        # Deadlines and SLO decisions, every --tick-ms, scheduled from their due time. Deadlines are judged only
        # once the reader has caught up: during a replay the outcomes may still be ahead in the log.
        nxt_t = time.monotonic()
        while True:
            nxt_t += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, nxt_t - time.monotonic()))
            if not caught_up[0]:
                continue
            now = time.time_ns()
            while due and due[0][0] + grace_ns <= now:
                dl, key = heapq.heappop(due)
                s = open_.get(key)
                if s is None:
                    continue                # succeeded or excluded before its deadline
                missing = [o for o in STORIES[s.st]["outcomes"] if o not in s.got]
                verdict(key, s, False, missing, dl, now)
                sweep_lag.add((now - dl) / 1e6)
            sec = now // 1_000_000_000
            for cohort in list(judge.dirty):
                r = judge.decide(cohort, sec)
                if r is None:
                    continue
                state, det = r
                n = judge.alerting[cohort]
                if state == "clear":
                    del judge.alerting[cohort]
                stats[state] += 1
                # a fire is timed from the deadline of the failed story that tipped it, a clear from the newest
                # verdict: the moment each became knowable
                tip = judge.tip_fail.get(cohort, 0) if state == "fire" else judge.tip[cohort]
                if (now - tip) / 1e9 > a.replay_alert_s:
                    continue                # rebuilt from long ago: nobody needs this alert now
                pending.append(SendMessage(msg(story=cohort[0], sf=cohort[1], state=state, alert=n, due_ns=tip,
                                               **det)))
            judge.dirty.clear()

    async def send_alerts():
        # Alerts are small and rare; one batch every 2 ms keeps sends out of the read loop. Once a second, one
        # SLI snapshot of every story type x surface goes out too, for dashboards.
        last_snap = time.monotonic()
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if caught_up[0] and time.monotonic() - last_snap >= 1:
                    last_snap = time.monotonic()
                    sec = time.time_ns() // 1_000_000_000
                    snap = {f"{st}/{sf}": round(judge.sli((st, sf), sec)[0], 4) for st, sf in judge.buckets}
                    pending.append(SendMessage(msg(state="sli", sli=snap)))
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
                print(f"[judge] catching up: replayed {int(stats['replayed']):,} earlier events this second, "
                      f"{len(open_):,} stories open", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r, sw = lat.flush(), sweep_lag.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            sec = time.time_ns() // 1_000_000_000
            slis = "  ".join(f"{st}/{sf} {judge.sli((st, sf), sec)[0]:.1%}" for st, sf in sorted(judge.buckets))
            alerting = ", ".join(f"{st} x {sf}" for st, sf in sorted(judge.alerting)) or "none"
            print(f"[judge] {r['n']:>6,} events/s  event-to-judge p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  stories judged ok {int(stats['ok']):,} "
                  f"failed {int(stats['failed']):,} excluded {int(stats['excluded']):,}  repeats dropped "
                  f"{int(stats['dupes'] + stats['after']):,}  late outcomes {int(stats['late']):,}  open "
                  f"{len(open_):,}  deadline-to-verdict p99 {sw['p99']:.0f}ms  SLI {slis}  alerts sent "
                  f"{int(stats['sent']):,} (fire {int(stats['fire']):,}, clear {int(stats['clear']):,})  "
                  f"alerting: {alerting}  {stalls.read()}  slowest event->broker {stats['to_broker']:.0f} ms, "
                  f"broker->judge {stats['from_broker']:.0f} ms", flush=True)
            stats.clear()
            cut = time.time_ns() - 2 * deadline_ns
            for k in [k for k, (t, _) in closed.items() if t < cut]:
                del closed[k]

    parts = list(range(PARTITIONS[STORY_EVENTS]))
    await asyncio.gather(tick(), send_alerts(), report(), stalls.watch(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="story-judge")
    p.add_argument("--budget-ms", type=int, default=50, help="event-to-judge p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the story-events partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--lookback-s", type=float, default=0,
                   help="on start, read from this long ago (default: the deadline plus 15 s)")
    p.add_argument("--deadline-s", type=float, default=60, help="a story fails if an outcome is missing this long "
                                                                "after its trigger")
    p.add_argument("--grace-ms", type=float, default=200, help="wait past a deadline for events already in flight")
    p.add_argument("--tick-ms", type=float, default=100, help="how often deadlines and SLOs are checked")
    p.add_argument("--window-s", type=int, default=60, help="seconds of verdicts per SLI")
    p.add_argument("--min-clients", type=int, default=50, help="clients needed before a cohort is judged")
    p.add_argument("--fire-sli", type=float, default=0.95, help="SLI below which an alert fires")
    p.add_argument("--clear-sli", type=float, default=0.985, help="SLI at or above which it clears")
    p.add_argument("--replay-alert-s", type=float, default=30,
                   help="after a restart, still send alerts for verdicts younger than this")
    args = p.parse_args()
    args.lookback_s = args.lookback_s or args.deadline_s + 15
    asyncio.run(main(args))
