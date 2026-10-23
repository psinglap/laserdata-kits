"""The alarm state service for camera partitions, fed by the camera partition lane.

Reads every partition of camera-partition-events directly, on --readers connections of its own, tracking its own
position (no consumer group: a group reader visits one partition per poll, so an event waits for its partition's
turn). On start it replays the lane from the first event, so a restarted service rebuilds every partition's armed
state and running countdowns from the log before it reports live latency; alarms that fell due during the replay
are still raised if they are younger than --replay-alert-s. With long retention you would add a periodic state
snapshot to replay from; the kit does not include one. Each record is one event for one alarm
partition, with that partition's event sequence: a camera detection, an arm, a disarm, or a panic press.

Per alarm partition it keeps: armed or not, and the running entry-delay countdown, if any.
  * detect while armed and no countdown running: the countdown starts from the detection's time, using the
    partition's entry delay (carried on the event); a delay of 0 raises at once.
  * disarm: cancels a running countdown. If the countdown has already run out, the alarm is raised first:
    a disarm that arrives late does not cancel an alarm that is already due, exactly as a person who disarms late.
  * arm: arms the partition; panic: raises a silent alarm at once, armed or not.
A timer task raises every countdown that runs out (checked every 2 ms). Raised alarms go to alarms-raised, which
every responder reads (sites.py plays the monitoring agents, notifications and the response workflows). Alarms
are sent in small batches from their own task, never from inside the read loop.

Prints event-to-state p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the countdowns.
"""
import argparse
import asyncio
import gc
import heapq
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (ARM, DEFAULT_URL, DETECT, DISARM, EVENTS, INTRUSION, PANIC, PARTITIONS, RAISED, SILENT, STREAM,
                    Stalls, Window, ensure_topics, load, msg)


class State:
    __slots__ = ("seq", "armed", "due", "cause", "src", "open")

    def __init__(self):
        self.seq, self.armed, self.due, self.cause, self.src, self.open = 0, 0, 0, 0, 0, 0


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), EVENTS, RAISED)
    out = await (await own_client()).producer(STREAM, RAISED, topic_partitions_count=PARTITIONS[RAISED])
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    st = defaultdict(State)                 # alarm partition -> its state
    timers = []                             # (due ns, alarm partition, cause seq): running entry delays
    pending = []                            # alarms waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays events from before it joined
    running = [0]                           # entry delays counting down now
    gc.freeze()                             # keep start-up objects out of the collector's way

    def raise_alarm(k, s, kind, cause, due_ns, src_ns, now):
        stats[kind] += 1
        running[0] -= bool(s.due)
        s.open, s.due = 1, 0                # one alarm per incident; the next arm or disarm closes it
        if now - due_ns > a.replay_alert_s * 1e9:
            return                          # replayed from long ago: state rebuilt, nobody needs this alarm now
        pending.append(SendMessage(msg(ap=k, kind=kind, cause=cause, due_ns=due_ns, src_ns=src_ns)))

    def on_event(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0] and not d.get("snap"):     # the start-up snapshot is not live traffic
            lat.add(age)
            b = m.timestamp() / 1e3                 # broker append time, ms: where the time went
            stats["to_broker"] = max(stats["to_broker"], b - d["ns"] / 1e6)
            stats["from_broker"] = max(stats["from_broker"], now / 1e6 - b)
        else:                               # still replaying: track the oldest event seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = d["ap"]
        s = st[k]
        if d["seq"] <= s.seq:
            stats["dropped"] += 1           # older or repeated event: already applied
            return
        s.seq = d["seq"]
        stats["events"] += 1
        kind = d["k"]
        if kind == DETECT:
            stats["detections"] += 1
            if s.armed and not s.due and not s.open:
                if d["delay_s"] <= 0:
                    raise_alarm(k, s, INTRUSION, d["seq"], d["ns"], d["ns"], now)
                else:
                    s.due, s.cause, s.src = d["ns"] + int(d["delay_s"] * 1e9), d["seq"], d["ns"]
                    heapq.heappush(timers, (s.due, k, s.cause))
                    running[0] += 1
                    stats["started"] += 1
        elif kind == DISARM:
            if s.due:
                t = now if caught_up[0] else d["ns"]    # while replaying, judge by the disarm's own time
                if t >= s.due:              # the countdown ran out before this disarm was applied
                    stats["late_disarms"] += 1
                    raise_alarm(k, s, INTRUSION, s.cause, s.due, s.src, now)
                else:
                    stats["cancelled"] += 1
                    stats["min_left"] = min(stats.get("min_left", 1e9), (s.due - t) / 1e6)
                    running[0] -= 1
            s.armed, s.due, s.open = 0, 0, 0
        elif kind == ARM:
            running[0] -= bool(s.due)
            s.armed, s.due, s.open = 1, 0, 0
        elif kind == PANIC:
            raise_alarm(k, s, SILENT, d["seq"], d["ns"], d["ns"], now)

    async def countdowns():
        # Raise every entry delay that has run out. Checked every 2 ms, from the due time, not from a poll.
        while True:
            if not caught_up[0]:            # replaying: a disarm for this countdown may not have been read yet
                await asyncio.sleep(0.002)
                continue
            now = time.time_ns()
            while timers and timers[0][0] <= now:
                due, k, cause = heapq.heappop(timers)
                s = st[k]
                if s.due == due and s.cause == cause:      # still running: not disarmed, not re-armed
                    raise_alarm(k, s, INTRUSION, cause, due, s.src, now)
            await asyncio.sleep(0.002)

    async def read(pids):
        # Each partition in turn, from the first event (the state is rebuilt from the log), then on from our own
        # position; nothing is stored on the broker per poll. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, EVENTS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_event(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_alarms():
        # Alarms are small and rare; one batch every 2 ms keeps sends out of the read loop.
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
                      f"state rebuilt for {len(st):,} alarm partitions", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            left = f"{stats['min_left'] / 1000:.2f} s" if "min_left" in stats else "-"
            print(f"[{a.name}] {r['n']:>7,} events/s  event-to-state p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  detections {int(stats['detections']):,}  "
                  f"entry delays started {int(stats['started']):,}, cancelled by a disarm {int(stats['cancelled']):,} "
                  f"(closest {left} before running out), disarms applied after the delay ran out "
                  f"{int(stats['late_disarms']):,}  alarms sent {int(stats['sent']):,} (intrusion "
                  f"{int(stats[INTRUSION]):,}, panic {int(stats[SILENT]):,})  countdowns running {running[0]:,}  "
                  f"{stalls.read()}  slowest event->broker {stats['to_broker']:.0f} ms, broker->state "
                  f"{stats['from_broker']:.0f} ms", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[EVENTS]))
    await asyncio.gather(send_alarms(), countdowns(), report(), stalls.watch(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="alarm-state")
    p.add_argument("--budget-ms", type=int, default=50, help="event-to-state p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the camera-partition-events partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--replay-alert-s", type=float, default=30,
                   help="after a restart, still raise alarms that fell due less than this long ago")
    asyncio.run(main(p.parse_args()))
