"""The rider alert service, fed by the rider events lane.

Reads every partition of rider-events directly, on --readers connections of its own, tracking its own position
and storing it on the broker at most once a second (no consumer group: a group reader visits one partition per
poll, so with 16 partitions an event waits for its partition's turn). Each record is one event from one scooter,
with the scooter's own clock (vt), its event sequence and its whole current state, so:
  * a resent event (the scooter never got the acknowledgement) has a sequence already applied: it changes
    nothing and raises no second alert;
  * an event older than common.FRESH_S on the scooter's clock is history, not news: it is filed, and no phone
    buzzes. The ingest step already keeps the reconnect backlog off this lane; this check holds even if it
    did not. A crash is the exception: it is always sent, and marked as delayed when it is late.

Alerts (stand-ins for yours; put your own in rules()): charging started, charging complete, movement while
parked (theft or tow), crash. They go to rider-alerts, which every notifier reads (scooters.py plays app push,
the SMS gateway and the support console), sent in small batches from their own task, never from the read loop.

Prints event-to-service p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the alerts.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (ALERT_KINDS, ALERTS, ALWAYS_SEND, CHARGE_DONE, CHARGE_START, CRASH, DEFAULT_URL, FRESH_S, LANE,
                    MOVED, PARTITIONS, STREAM, Stalls, Window, ensure_topics, load, msg)


def rules(d: dict, now_ns: int):
    """Returns (alert kind, delayed) for an event that should reach the rider now, or None."""
    if d["k"] not in ALERT_KINDS:
        return None
    late = now_ns - d["vt"] > FRESH_S * 1e9
    if late and d["k"] not in ALWAYS_SEND:
        return None
    return d["k"], late


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LANE, ALERTS)
    out = await (await own_client()).producer(STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS])
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    seq = {}                                # scooter -> last event sequence applied
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
        k = d["s"]
        if d["seq"] <= seq.get(k, 0):
            stats["repeats"] += 1           # resent or replayed: already applied, no second alert
            return
        seq[k] = d["seq"]
        stats["events"] += 1
        r = rules(d, now)
        if r is None:
            if d["k"] in ALERT_KINDS:
                stats["filed"] += 1         # late on the scooter's clock: history, not news
            return
        kind, delayed = r
        stats[kind] += 1
        stats["delayed"] += delayed
        if age > a.replay_alert_s * 1000:
            return                          # replayed from long ago: state rebuilt, nobody needs this alert now
        pending.append(SendMessage(msg(s=k, seq=d["seq"], kind=kind, delayed=int(delayed), vt=d["vt"],
                                       src_ns=d["ns"], soc=d["soc"], lat=d["lat"], lon=d["lon"])))

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
                ms = await c.poll_messages(STREAM, LANE, consumer=who[p], partition_id=p, count=a.batch,
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
                      f"state rebuilt for {len(seq):,} scooters", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} events/s  event-to-service p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  alerts sent {int(stats['sent']):,} "
                  f"(charging started {int(stats[CHARGE_START]):,}, complete {int(stats[CHARGE_DONE]):,}, "
                  f"parked movement {int(stats[MOVED]):,}, crash {int(stats[CRASH]):,}, delayed {int(stats['delayed']):,})  "
                  f"late events filed, not pushed {int(stats['filed']):,}  repeats dropped {int(stats['repeats']):,}  "
                  f"{stalls.read()}  slowest event->broker {stats['to_broker']:.0f} ms, broker->service "
                  f"{stats['from_broker']:.0f} ms", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[LANE]))
    await asyncio.gather(send_alerts(), report(), stalls.watch(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="alert-service")
    p.add_argument("--budget-ms", type=int, default=50, help="event-to-service p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the rider-events partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--commit-s", type=float, default=1, help="store each partition's offset at most this often")
    p.add_argument("--replay-alert-s", type=float, default=30,
                   help="after a restart, still send alerts for events younger than this")
    asyncio.run(main(p.parse_args()))
