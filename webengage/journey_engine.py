"""A journey worker: owns a fixed set of partitions of the tracked-events topic and advances every
Contextual Journey trip of the users on them.

Worker --worker of --workers owns every partition p with p % workers == worker and reads each one
directly (no consumer group, no rebalance), in order. Because events are keyed by user, every event
of a user, for every one of the user's trips, reaches this one worker, one after another.

Per event (the trip is user + context value, as in Contextual Journeys):
  * an entry event starts a trip and schedules its reminder --wait-s later (a second entry for a
    trip that is still running is ignored: one trip per user per context);
  * the trip's exit event ends it; if the reminder was not sent yet, it is cancelled;
  * activity events for the same context are counted on the trip.
A timer sends each due reminder whose trip is still running to the journey-sends topic, keyed by
user, from its own task every --flush-ms, so the reader never waits on a send. Offsets are stored on
the broker at most once a second per partition, so whichever worker owns a partition next resumes
from it (events after the last stored offset are read again; the per-user sequence drops them).

Prints a line a second:
  * event-to-trip latency (written by the SDK to applied to the trip here), PASS or FAIL against --budget-ms;
  * trips entered, reminders sent, reminders cancelled by an exit that arrived first;
  * order violations, missing events, and users whose events came in on another partition
    (all three are 0 when events are keyed by user);
  * trips running, and users with two or more trips running at once.
"""
import argparse
import asyncio
import gc
import heapq
import time
from collections import defaultdict

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, EVENTS_TOPIC, JOURNEYS, ROLE, SENDS_TOPIC, STREAM, Window, connect, ensure_topic,
                    load, msg, owner, part)


async def main(a):
    name = f"journey-{a.worker}"
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, EVENTS_TOPIC, a.partitions)     # whoever starts first creates the topics
    await ensure_topic(admin, STREAM, SENDS_TOPIC, a.send_partitions)
    parts = (await admin.get_topic(STREAM, EVENTS_TOPIC)).partitions_count
    send_parts = (await admin.get_topic(STREAM, SENDS_TOPIC)).partitions_count
    mine = [p for p in range(parts) if owner(p, a.workers) == a.worker]
    budget_ns = a.budget_ms * 1_000_000
    wait_ns = int(a.wait_s * 1e9)
    last_seq, seen_on = {}, {}
    trips = {}                                            # (user, context) -> trip state
    per_user = defaultdict(int)                           # user -> trips running
    timers = []                                           # (reminder due ns, user, context)
    lat = Window()
    st, total = defaultdict(int), defaultdict(int)
    caught_up = [False]                                   # a new reader first replays what was written before it
    out = defaultdict(list)                               # reminders waiting for the next flush
    gc.freeze()                                           # keep start-up objects out of the collector's way

    def end(key, u):
        trips.pop(key, None)
        per_user[u] -= 1
        if per_user[u] <= 0:
            per_user.pop(u, None)

    def apply(e, now):
        u, seq = e["u"], e["seq"]
        if seen_on.setdefault(u, e["_p"]) != e["_p"]:
            total["moved"] += 1
        last = last_seq.get(u, 0)
        if seq <= last:
            total["out_of_order" if seq < last else "repeats"] += 1
            return
        if seq > last + 1 and last:
            total["lost"] += seq - last - 1
        last_seq[u] = seq
        role = ROLE.get(e["e"])
        if role:
            j, kind = role
            key = (u, e["c"])
            t = trips.get(key)
            if kind == "entry":
                if t is None:
                    trips[key] = {"j": j, "entry": e["ns"], "due": e["ns"] + wait_ns, "sent": False, "acts": 0}
                    per_user[u] += 1
                    heapq.heappush(timers, (e["ns"] + wait_ns, u, e["c"]))
                    st["entered"] += 1
                else:
                    st["reentry_ignored"] += 1             # one trip per user per context at a time
            elif kind == "exit" and t is not None:
                if t["sent"]:
                    st["exit_after"] += 1                  # converted after the reminder
                else:
                    st["cancelled"] += 1                   # the exit won the race: no reminder for this trip
                end(key, u)
            elif t is not None:
                t["acts"] += 1
        age = now - e["ns"]
        if not caught_up[0]:
            st["replayed"] += 1                           # written before this worker started: not live latency
            st["max_age"] = max(st["max_age"], age)
        else:
            lat.add(age / 1e6)
        st["events"] += 1

    async def reader(plist):
        c = await connect(a.url)
        consumers = {p: Consumer.Single(f"journey-p{p}") for p in plist}
        nxt, committed = {}, {}
        while True:
            more = False
            for p in plist:
                start = PollingStrategy.Offset(nxt[p]) if p in nxt else PollingStrategy.Next()
                # Store this partition's offset on the broker at most once a second, not on every poll.
                commit = time.monotonic() - committed.get(p, 0) >= a.commit_s
                ms = await c.poll_messages(STREAM, EVENTS_TOPIC, consumer=consumers[p], partition_id=p,
                                           count=a.batch, auto_commit=commit, polling_strategy=start)
                if ms:
                    if commit:
                        committed[p] = time.monotonic()
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        e = load(m.payload())
                        e["_p"] = p
                        apply(e, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def timer():
        # Reminders fire from their due time; the trip state decides whether one still goes out.
        nxt = time.monotonic()
        while True:
            now = time.time_ns()
            while timers and timers[0][0] <= now:
                due, u, c = heapq.heappop(timers)
                t = trips.get((u, c))
                if t is None or t["sent"] or t["due"] != due:
                    continue                              # exited first: cancelled, counted in apply()
                t["sent"] = True
                out[part(u, send_parts)].append(SendMessage(msg(
                    ns=now, due=due, entry=t["entry"], u=u, c=c, j=t["j"], kind=JOURNEYS[t["j"]]["reminder"],
                    w=a.worker)))
                st["reminders"] += 1
            nxt = max(nxt + a.flush_ms / 1000, time.monotonic() - 0.1)   # from the due time, not from "now"
            await asyncio.sleep(max(0.0, nxt - time.monotonic()))

    async def sender():
        # Reminders go out on their own connection, one batch per send partition per tick.
        w = await connect(a.url)
        while True:
            await asyncio.sleep(a.flush_ms / 1000)
            if out:
                batch = dict(out)
                out.clear()
                await asyncio.gather(*(w.send_messages(STREAM, SENDS_TOPIC, Partitioning.partition_id(pid), ms)
                                       for pid, ms in batch.items()))

    async def sweep():
        # Trips that got their reminder and never converted end --linger-s after it.
        while True:
            await asyncio.sleep(1)
            cut = time.time_ns() - int(a.linger_s * 1e9)
            for key in [k for k, t in trips.items() if t["sent"] and t["due"] < cut]:
                end(key, key[0])

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the replay is finished
                if st["replayed"] and st["max_age"] < budget_ns:
                    caught_up[0] = True
                print(f"[{name}] catching up: replayed {st['replayed']:,} earlier events this second", flush=True)
                st.clear()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            multi = sum(1 for n in per_user.values() if n >= 2)
            print(f"[{name}] {r['n']:>6,} events/s applied  event-to-trip p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"trips entered {st['entered']:>5,}/s  reminders {st['reminders']:>5,}/s  "
                  f"cancelled by exit {st['cancelled']:>5,}/s  "
                  f"out of order {total['out_of_order']:,}  missing {total['lost']:,}  "
                  f"on another partition {total['moved']:,}  trips running {len(trips):,}  "
                  f"users in 2+ trips {multi:,}", flush=True)
            st.clear()

    groups = [mine[r::a.readers] for r in range(a.readers)]
    print(f"[{name}] owns partitions {mine} of {parts} (worker {a.worker} of {a.workers}), "
          f"reading on {a.readers} connection(s), reminder {a.wait_s:g} s after entry", flush=True)
    await asyncio.gather(report(), timer(), sender(), sweep(), *(reader(g) for g in groups if g))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--worker", type=int, default=0)
    p.add_argument("--workers", type=int, default=4, help="journey workers splitting the partitions")
    p.add_argument("--partitions", type=int, default=64, help="partitions for a new events topic")
    p.add_argument("--send-partitions", type=int, default=16, help="partitions for a new journey-sends topic")
    p.add_argument("--readers", type=int, default=1, help="connections reading this worker's partitions")
    p.add_argument("--wait-s", type=float, default=2.0, help="same as sdk_events.py")
    p.add_argument("--linger-s", type=float, default=5.0, help="how long a reminded trip waits for a late exit")
    p.add_argument("--budget-ms", type=float, default=50, help="p99 budget, event written to trip updated")
    p.add_argument("--poll-ms", type=float, default=5, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--flush-ms", type=float, default=10, help="how often due reminders are checked and sent")
    p.add_argument("--commit-s", type=float, default=1, help="how often each partition's offset is stored")
    asyncio.run(main(p.parse_args()))
