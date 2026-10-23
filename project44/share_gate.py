"""The share gate, fed by the position share lane.

Carriers connect their whole fleet; the gate decides which customer sees which position. It reads every partition
of fleet-positions directly, on --readers connections of its own, from the start of the retained lane and then
from its own position (no consumer group: a group reader visits one partition per poll, so an event can wait for
its partition's turn). Starting from the retained lane is how a restarted gate rebuilds every truck's open
shipments in seconds. Each record is one event on one vehicle, keyed by vehicle:
  * assign: a shipment is put on this truck (customer, pick-up and delivery locations, time window);
  * stop: the shipment is no longer on this truck (the carrier stopped sharing, the load was cancelled or moved to
    another tractor);
  * pos: a position from the truck's ELD or telematics device.
A truck's events stay in order in one partition, so a stop is always applied before the next position: no position
is shared with a customer after their shipment has left the truck. Every record carries the truck's event
sequence, so a replayed or repeated event changes nothing.

The rule is project44's published data-sharing rule (common.step): open when the right truck enters the 50 km
outer geofence around the pick-up inside the time window, close when it drives out of the delivery geofence or
stays inside it too long. Shared positions, "tracking started" and "completed" go to shipment-updates, which the
customer push, the visibility screens and the ETA service read (fleet.py plays them). Updates are sent in small
batches from their own task, never from inside the read loop.

Prints position-to-gate p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and what was shared.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (DEFAULT_URL, DONE, LANE, PARTITIONS, PENDING, STREAM, UPDATES, Stalls, Window, ensure_topics,
                    load, msg, step)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LANE, UPDATES)
    out = await (await own_client()).producer(STREAM, UPDATES, topic_partitions_count=PARTITIONS[UPDATES])
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    trucks = {}                             # vehicle -> [last sequence applied, {shipment: [customer, state list]}]
    pending = []                            # updates waiting for the next send (one batch every 2 ms)
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
        v = d["v"]
        st = trucks.get(v)
        if st is None:
            st = trucks[v] = [0, {}]
        if d["seq"] <= st[0]:
            stats["dropped"] += 1           # older or repeated event: already applied
            return
        st[0] = d["seq"]
        k = d["k"]
        if k == "assign":
            st[1][d["sh"]] = [d["c"], [PENDING, tuple(d["pu"]), tuple(d["dr"]), d["win"], None]]
            stats["assigned"] += 1
            return
        if k == "stop":
            if st[1].pop(d["sh"], None) is not None:
                stats["stopped"] += 1
            return
        stats["positions"] += 1
        live = age <= a.replay_send_s * 1000    # replayed from long ago: state rebuilt, nobody needs it now
        shared = False
        for sh, (c, s) in list(st[1].items()):
            ev, share = step(s, d["lat"], d["lon"], d["ns"], a.dwell_s)
            if ev is not None:
                stats[ev] += 1
                if live:
                    pending.append(SendMessage(msg(k=ev, v=v, sh=sh, c=c, pseq=d["seq"], src_ns=d["ns"])))
            if share:
                shared = True
                stats["shared"] += 1
                if live:
                    pending.append(SendMessage(msg(k="pos", v=v, sh=sh, c=c, pseq=d["seq"], lat=d["lat"],
                                                   lon=d["lon"], src_ns=d["ns"])))
            if s[0] == DONE:
                del st[1][sh]
        if not shared:
            stats["withheld"] += 1          # no open shipment on this truck: the position goes to no customer

    async def read(pids):
        # Each partition in turn, from the start of the retained lane, then on from our own position (nothing is
        # stored on the broker per poll; an offset write on every poll stalls the lane). A full batch means more
        # is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, LANE, consumer=who[p], partition_id=p, count=a.batch,
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

    async def send_updates():
        # Updates are small and come in bursts; one batch every 2 ms keeps sends out of the read loop.
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
            open_sh = sum(len(s[1]) for s in trucks.values())
            if not caught_up[0]:
                # caught up once a whole second of events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier events this second, "
                      f"state rebuilt for {len(trucks):,} trucks, {open_sh:,} shipments", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} events/s  position-to-gate p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  positions {int(stats['positions']):,}  "
                  f"shared {int(stats['shared']):,}  withheld {int(stats['withheld']):,}  tracking started "
                  f"{int(stats['start']):,}  completed {int(stats['end']):,}  assigned {int(stats['assigned']):,}  "
                  f"stopped {int(stats['stopped']):,}  updates sent {int(stats['sent']):,}  open shipments "
                  f"{open_sh:,}  {stalls.read()}  slowest event->broker {stats['to_broker']:.0f} ms, "
                  f"broker->gate {stats['from_broker']:.0f} ms", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[LANE]))
    await asyncio.gather(send_updates(), report(), stalls.watch(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="share-gate")
    p.add_argument("--budget-ms", type=int, default=100, help="position-to-gate p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the fleet-positions partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--dwell-s", type=float, default=3 * 3600,
                   help="close a shipment whose truck stays inside the delivery geofence this long")
    p.add_argument("--replay-send-s", type=float, default=30,
                   help="after a restart, still send updates for positions younger than this")
    asyncio.run(main(p.parse_args()))
