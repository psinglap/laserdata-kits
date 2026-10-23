"""One Destination behind the Route, reading the shared log at its own pace from its own offsets.

Run one per Destination: the SIEM (--dest splunk_lb), Cribl Lake (--dest cribl_lake), a new SIEM
during a migration (--dest new_siem --backfill). Each reads every partition directly (its own
offsets, no consumer group, so no rebalances) on its own connection, so no Destination can slow
another, and none can push back on the Worker Group.

When the Destination is down (--outage-at, --outage-for) it stops reading and its offsets stay
where they were; its events wait in the log, not on a Worker Process's disk. When it is back,
fresh events go first: the live reader jumps to the end of the log and is timed against
--budget-ms at once, while a second reader drains the held range behind it at --drain-rate. With
--backfill, a new Destination starts the same way: live from the end, and everything still in the
log drained behind it.

Every event carries a per-host sequence, so the Destination drops repeats and counts any gap as
lost, across the live and drained ranges. Prints a line a second:
  * live events delivered, event-to-destination p50/p99/p99.9, PASS or FAIL against --budget-ms;
  * events waiting in the log for this Destination, the drain rate and the oldest event drained;
  * events lost (sequence gaps) and repeats dropped;
  * while down: events waiting and how long; when the drain finishes: how long it took and events lost.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, STREAM, TOPIC, Window, connect, ends, load


async def main(a):
    name = a.dest
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, TOPIC):
        print(f"[{name}] waiting for the Worker Group to create {STREAM}/{TOPIC}…", flush=True)
        await asyncio.sleep(1)
    live_c, drain_c = await connect(a.url), await connect(a.url)
    me = Consumer.Single(f"dest-{name}")
    tail = await ends(admin, STREAM, TOPIC)
    parts = len(tail)
    live_next = list(tail)                           # live starts at the end of the log: only new events are timed
    drain = {}                                       # partition -> [next, end): the held range, read behind live
    seq_live, seq_drain, first_live = {}, {}, {}
    from_start = not any(tail)                       # started before the first event: every host starts at seq 1
    st, total = defaultdict(int), defaultdict(int)
    lat = Window()
    late = [0]
    size = [0.0]                                     # mean stored size of an event in this topic, for bytes waiting
    oldest = [0.0]
    t0 = time.monotonic()
    out_from, out_to = a.outage_at, a.outage_at + a.outage_for
    phase = {"down": False, "drain_started": None}
    budget_ns = a.budget_ms * 1_000_000
    gc.freeze()                                      # keep startup tables out of the collector's way

    async def refresh_size():
        t = await admin.get_topic(STREAM, TOPIC)
        if t and t.messages_count:
            size[0] = t.size / t.messages_count

    def is_down():
        return a.outage_for > 0 and out_from <= time.monotonic() - t0 < out_to

    def start_drain(frm, to, why):
        # Everything in [frm, to) per partition is delivered behind live; live carries on from `to`.
        nonlocal seq_live
        for pid in range(parts):
            if to[pid] > frm[pid]:
                drain[pid] = [frm[pid], to[pid]]
        seq_drain.clear()
        seq_drain.update(seq_live)                   # the drain continues each host's sequence where it stopped
        seq_live = {}
        first_live.clear()
        phase["drain_started"] = time.monotonic()
        phase["drained0"] = total["drained"]
        n = sum(e - s for s, e in drain.values())
        print(f"[{name}] {why}: {n:,} events ({n * size[0] / 1e6:.1f} MB) to drain behind live at "
              f"{a.drain_rate:,.0f}/s; fresh events go first", flush=True)
        if not drain:
            finish_drain()

    def finish_drain():
        nonlocal seq_live
        lost = 0
        for h, first in first_live.items():          # where live began for each host must follow where drain ended
            gap = first - seq_drain.get(h, 0) - 1
            if gap > 0:
                lost += gap
            elif gap < 0:
                total["overlap"] += -gap
        for h, s in seq_drain.items():
            seq_live.setdefault(h, s)                # hosts live hasn't seen yet continue from the drain
        first_live.clear()
        total["lost"] += lost
        took = time.monotonic() - phase["drain_started"]
        phase["drain_started"] = None
        print(f"[{name}] backlog drained: {total['drained'] - phase['drained0']:,} events in {took:.1f} s, fresh events first "
              f"throughout, {total['lost']:,} events lost", flush=True)

    def apply_live(ms, now):
        for m in ms:
            e = load(m.payload())
            h, s = e["host"], e["seq"]
            last = seq_live.get(h)
            if last is None:
                if phase["drain_started"] is not None:
                    first_live[h] = s                # checked against the drain when it finishes
                elif from_start and s > 1:
                    total["lost"] += s - 1
            elif s <= last:
                st["repeats"] += 1
                continue
            elif s > last + 1:
                total["lost"] += s - last - 1
            seq_live[h] = s
            age = now - e["ns"]
            lat.add(age / 1e6)
            if age > budget_ns:
                late[0] += 1
            st["live"] += 1

    def apply_drain(ms, now):
        for m in ms:
            e = load(m.payload())
            h, s = e["host"], e["seq"]
            last = seq_drain.get(h, 0)
            if s <= last:
                st["repeats"] += 1
                continue
            if s > last + 1:
                total["lost"] += s - last - 1
            seq_drain[h] = s
            oldest[0] = max(oldest[0], (now - e["ns"]) / 1e9)
            st["drained"] += 1
            total["drained"] += 1

    async def live():
        while True:
            if is_down():
                if not phase["down"]:
                    phase["down"] = True
                    print(f"[{name}] destination down: reading stopped, offsets held", flush=True)
                await asyncio.sleep(0.05)
                continue
            if phase["down"]:                        # back: fresh events first, the held range behind them
                phase["down"] = False
                now_end = await ends(admin, STREAM, TOPIC)
                held = list(live_next)
                live_next[:] = now_end
                await refresh_size()
                start_drain(held, now_end, "destination back")
            more = False
            for pid in range(parts):
                ms = await live_c.poll_messages(STREAM, TOPIC, consumer=me, partition_id=pid, count=a.batch,
                                                auto_commit=False, polling_strategy=PollingStrategy.Offset(live_next[pid]))
                if ms:
                    live_next[pid] = ms[-1].offset() + 1
                    apply_live(ms, time.time_ns())
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def drainer():
        tokens, refilled = 0.0, time.monotonic()
        while True:
            if not drain:
                await asyncio.sleep(0.02)
                refilled = time.monotonic()
                continue
            now = time.monotonic()
            tokens = min(a.drain_rate * 0.1, tokens + (now - refilled) * a.drain_rate)
            refilled = now
            for pid in list(drain):
                if int(tokens) < 1:
                    break
                s, e = drain[pid]
                ms = await drain_c.poll_messages(STREAM, TOPIC, consumer=me, partition_id=pid,
                                                 count=min(a.batch, e - s, int(tokens)), auto_commit=False,
                                                 polling_strategy=PollingStrategy.Offset(s))
                ms = [m for m in ms if m.offset() < e]
                if ms:
                    tokens -= len(ms)
                    drain[pid][0] = ms[-1].offset() + 1
                    apply_drain(ms, time.time_ns())
                if drain[pid][0] >= e:
                    del drain[pid]
            if not drain:
                finish_drain()
            await asyncio.sleep(0.02)

    async def report():
        while True:
            await asyncio.sleep(1)
            await refresh_size()
            if phase["down"]:
                end = await ends(admin, STREAM, TOPIC)
                w = sum(e - n for e, n in zip(end, live_next))
                lat.flush()
                print(f"[{name}] destination down: delivered 0/s  waiting {w:,} in the log "
                      f"({w * size[0] / 1e6:.1f} MB), offsets held  down for {time.monotonic() - t0 - out_from:4.1f} s  "
                      f"lost {total['lost']:,}", flush=True)
                st.clear()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            line = (f"[{name}] {r['n']:>6,} events/s live to {name}  event-to-destination p50 {r['p50']:6.1f}ms  "
                    f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                    f"over budget {late[0]:,}  lost {total['lost']:,}  repeats dropped {st['repeats']:,}")
            if phase["drain_started"] is not None:
                w = sum(e - s for s, e in drain.values())
                line += (f"  | drained {st['drained']:,}/s behind live, {w:,} waiting ({w * size[0] / 1e6:.1f} MB), "
                         f"oldest {oldest[0]:5.1f} s old")
            print(line, flush=True)
            late[0] = 0
            oldest[0] = 0.0
            st.clear()

    print(f"[{name}] reading {STREAM}/{TOPIC}, {parts} partitions, live from the end of the log", flush=True)
    if a.backfill:
        await refresh_size()
        start_drain([0] * parts, tail, "backfill from the first offset")
    await asyncio.gather(live(), drainer(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--dest", default="splunk_lb", help="Destination name, as in the Worker Group's outputs")
    p.add_argument("--backfill", action="store_true", help="a new Destination: drain everything in the log behind live")
    p.add_argument("--drain-rate", type=float, default=10_000, help="events a second drained behind live")
    p.add_argument("--outage-at", type=float, default=60, help="seconds after start the Destination goes down")
    p.add_argument("--outage-for", type=float, default=0, help="outage length in seconds (0 = none)")
    p.add_argument("--budget-ms", type=float, default=100, help="p99 budget, Worker to Destination, live events")
    p.add_argument("--poll-ms", type=float, default=5, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
