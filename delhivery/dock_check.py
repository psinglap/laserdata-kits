"""The load check, fed by the dock scan lane.

Reads every partition of dock-scans directly, on --readers connections of its own, tracking its own position and
storing it on the broker at most once a second (no consumer group: a group reader visits one partition per poll,
so an event can wait for its partition's turn). Each record is one event on one outbound trip, keyed by trip:
  * plan: the trip's current destinations (the whole list, never a change to it), from linehaul planning;
  * load: a bag scanned onto the trailer at the dock, with its destination and the loader's handheld;
  * seal: the trailer is closed and leaves.
A trip's events stay in order in one partition, so a plan revision is always applied before the load scans that
follow it: the check never judges a bag against a plan that has already changed. Every record carries the trip's
event sequence, so a replayed or repeated event changes nothing.

One rule (a stand-in for yours; put your own in check()): a bag whose destination is not on the trip's current
plan is a misload. The alert names the scan it came from and goes to dock-alerts, which every handheld and screen
reads (docks.py plays the loader's handheld, the dock supervisor and the yard view). Alerts are sent in small
batches from their own task, never from inside the read loop.

Prints scan-to-check p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the misloads flagged.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import ALERTS, DEFAULT_URL, DOCK, MISLOAD, PARTITIONS, STREAM, Stalls, Window, ensure_topics, load, msg


def check(plan: set, d: dict):
    """Returns an alert kind or None for one load scan against the trip's current plan."""
    if d["dest"] not in plan:
        return MISLOAD
    return None


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), DOCK, ALERTS)
    out = await (await own_client()).producer(STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS])
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    trips = {}                              # trip -> [last sequence applied, current plan]
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
        t = d["t"]
        st = trips.get(t)
        if st is not None and d["seq"] <= st[0]:
            stats["dropped"] += 1           # older or repeated event: already applied
            return
        if d["k"] == "plan":
            trips[t] = [d["seq"], set(d["dests"])]
            stats["revisions"] += d["rev"] > 1
            return
        if st is None:
            return                          # a trip whose plan this reader never saw (started mid-trip)
        st[0] = d["seq"]
        if d["k"] == "seal":
            del trips[t]                    # the trailer has left; its state is no longer needed
            stats["seals"] += 1
            return
        stats["loads"] += 1
        kind = check(st[1], d)
        if kind is None:
            return
        stats[kind] += 1
        if age > a.replay_alert_s * 1000:
            return                          # replayed from long ago: state rebuilt, nobody needs this alert now
        pending.append(SendMessage(msg(t=t, kind=kind, bag=d["bag"], dest=d["dest"], ldr=d["ldr"],
                                       scan_seq=d["seq"], src_ns=d["ns"])))

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
                ms = await c.poll_messages(STREAM, DOCK, consumer=who[p], partition_id=p, count=a.batch,
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
        # Alerts are small and come in bursts; one batch every 2 ms keeps sends out of the read loop.
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
                      f"state rebuilt for {len(trips):,} open trips", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} events/s  scan-to-check p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  load scans {int(stats['loads']):,}  "
                  f"plan revisions {int(stats['revisions']):,}  seals {int(stats['seals']):,}  "
                  f"misloads flagged {int(stats[MISLOAD]):,}  alerts sent {int(stats['sent']):,}  "
                  f"open trips {len(trips):,}  {stalls.read()}  slowest event->broker "
                  f"{stats['to_broker']:.0f} ms, broker->check {stats['from_broker']:.0f} ms", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[DOCK]))
    await asyncio.gather(send_alerts(), report(), stalls.watch(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="dock-check")
    p.add_argument("--budget-ms", type=int, default=100, help="scan-to-check p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the dock-scans partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--commit-s", type=float, default=1, help="store each partition's offset at most this often")
    p.add_argument("--replay-alert-s", type=float, default=30,
                   help="after a restart, still send alerts for scans younger than this")
    asyncio.run(main(p.parse_args()))
