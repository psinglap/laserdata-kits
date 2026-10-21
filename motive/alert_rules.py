"""The cargo alert rules, fed by the cargo sensor lane.

Reads every partition of cargo-sensor-events directly, on --readers connections of its own, tracking its own
position (no consumer group: a group reader visits one partition per poll, so with 16 partitions an event waited
for its partition's turn and a busy host pushed the tail past the budget). Each record is one event from one asset's
gateway: a door change (sent the moment the door opens or closes) or a one-minute reading. Every record carries
the asset's whole current state (door open or closed and since when, cargo temperature, set point, position) and
the asset's event sequence, so a replayed or repeated event is recognised and changes nothing, and a restarted
rules process needs no history to be right again.

Two rules (stand-ins for yours; put your own in rules()):
  * DOOR_OUTSIDE: a cargo door opened outside every approved geofence (yard, customer site);
  * TEMP_HIGH: cargo temperature rose above the asset's range. If a door changed in the last 15 minutes, the
    alert names that door event as the likely cause: the "why" next to the "what".
Alerts go to cargo-alerts, which every notifier reads (fleet.py plays the driver app, the fleet dashboard and
webhooks). Alerts are sent in small batches from their own task, never from inside the read loop.

Prints event-to-rules p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the alerts.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (ALERTS, DEFAULT_URL, DOOR_CAUSE_S, DOOR_OUTSIDE, PARTITIONS, READINGS, STREAM, TEMP_BAND,
                    TEMP_HIGH, Stalls, Window, ensure_topics, inside_approved, load, msg)


def rules(d: dict, hot: bool):
    """Returns (alert kind, likely cause) or None. `hot` is whether this asset is already in a temperature
    excursion, so one excursion raises one alert, not one a minute."""
    if d["k"] == "door" and d["door"] and not inside_approved(d["a"], (d["lat"], d["lon"])):
        return DOOR_OUTSIDE, None
    if d["temp"] > d["sp"] + TEMP_BAND and not hot:
        recent_door = d["door"] or (d["ns"] - d["door_ns"]) < DOOR_CAUSE_S * 1e9
        return TEMP_HIGH, ("door" if recent_door else None)
    return None


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), READINGS, ALERTS)
    out = await (await own_client()).producer(STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS])
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    seq = {}                                # asset -> last event sequence applied
    hot = set()                             # assets in a temperature excursion
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
        k = d["a"]
        if d["seq"] <= seq.get(k, 0):
            stats["dropped"] += 1           # older or repeated event: already applied
            return
        seq[k] = d["seq"]
        stats["events"] += 1
        stats["doors"] += d["k"] == "door"
        r = rules(d, k in hot)
        if d["temp"] <= d["sp"] + TEMP_BAND:
            hot.discard(k)
        if r is None:
            return
        kind, cause = r
        if kind == TEMP_HIGH:
            hot.add(k)
            stats["temp_door"] += cause == "door"
        stats[kind] += 1
        if age > a.replay_alert_s * 1000:
            return                          # replayed from long ago: state rebuilt, nobody needs this alert now
        pending.append(SendMessage(msg(a=k, kind=kind, cause=cause, ep=d["seq"], src_ns=d["ns"], temp=d["temp"],
                                       lat=d["lat"], lon=d["lon"])))

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, READINGS, consumer=who[p], partition_id=p, count=a.batch,
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
                      f"state rebuilt for {len(seq):,} assets", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} events/s  event-to-rules p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  door changes {int(stats['doors']):,}  "
                  f"alerts sent {int(stats['sent']):,} (door outside geofence {int(stats[DOOR_OUTSIDE]):,}, "
                  f"temperature {int(stats[TEMP_HIGH]):,}, cause named {int(stats['temp_door']):,})  "
                  f"assets in excursion {len(hot):,}  {stalls.read()}  slowest event->broker "
                  f"{stats['to_broker']:.0f} ms, broker->rules {stats['from_broker']:.0f} ms", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[READINGS]))
    await asyncio.gather(send_alerts(), report(), stalls.watch(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="alert-rules")
    p.add_argument("--budget-ms", type=int, default=50, help="event-to-rules p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the cargo-sensor-events partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--replay-alert-s", type=float, default=30,
                   help="after a restart, still send alerts for events younger than this")
    asyncio.run(main(p.parse_args()))
