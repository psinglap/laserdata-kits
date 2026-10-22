"""A NexVerse.ai bid node: reads every floor change from the lane and keeps the live floor table it
runs Dailyhunt auctions against.

Every node reads every partition with its own connection and its own offsets (no shared consumer group),
so adding a node never slows the others: each one gets every floor, in order per placement. The table is
placement -> floor in rupees CPM; the auction would read it when it compares bids with the floor.

Stands in for the auction path by counting auctions: the node runs --auctions-per-s auctions spread over
the placements. Prints a line a second:
  * floor-service-to-node latency (floor set -> applied here), with PASS or FAIL against --budget-ms;
  * stale-floor auctions: auctions this node ran on a placement after its floor stepped, before the node
    had the new one (latency x the node's auction rate on that placement), per second and per million;
    after a raise they sell below the new floor (and the rupees CPM they give away, at --fill), after a
    drop they go unfilled under a floor that is now too high; next to what the same steps would cost if
    the node picked floors up a second later;
  * gaps: a missing sequence number for a placement (must stay 0).

On start it rebuilds the table by replaying the last --replay-s seconds of the log (one rolling snapshot
from floors.py and the changes since); replayed messages are not timed as live.
"""
import argparse
import asyncio
import gc
import time

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, FLOORS, LANGS, PARTITIONS, STREAM, Window, connect, load


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, FLOORS):
        print(f"[{a.name}] waiting for floors.py to create the floor lane…", flush=True)
        await asyncio.sleep(1)

    total = a.langs * a.placements_per_lang
    per_pl = a.auctions_per_s / total             # auctions per second on one placement, on this node
    table = {}                                    # placement -> [seq, floor]
    lat = Window()
    st = dict(live=0, steps=0, below=0.0, unfilled=0.0, rs=0.0, stale_1s=0.0, rs_1s=0.0, gaps=0, replayed=0,
              sec_n=0, sec_age=0, last_old=0.0)
    started, caught, live_seen = time.monotonic(), [None], set()
    budget_ns = a.budget_ms * 1_000_000
    gc.freeze()                                   # keep start-up objects out of the collector's way

    def apply(ms, now):
        for m in ms:
            d = load(m.payload())
            k, seq, ns = d["k"], d["q"], d["ns"]
            prev = table.get(k)
            if prev is not None:
                if seq <= prev[0]:
                    continue                      # a repeat (rolling snapshot or at-least-once redelivery): skip it
                if seq > prev[0] + 1:
                    st["gaps"] += 1               # a floor change for this placement never arrived
            old = prev[1] if prev is not None else d["f"]
            table[k] = [seq, d["f"]]
            if caught[0] is None:
                # Still replaying: a new node first reads floors set before it started. They rebuild the
                # table but are not live latency. Track this second's oldest one to know when replay is over.
                st["replayed"] += 1
                st["sec_n"] += 1
                st["sec_age"] = max(st["sec_age"], now - ns)
                if now - ns >= budget_ns:
                    st["last_old"] = time.monotonic()
                continue
            if seq == 1:
                continue                          # the start-up snapshot is not a live change
            delay = (now - ns) / 1e9
            lat.add(delay * 1e3)
            st["live"] += 1
            live_seen.add(k)
            if d["s"]:
                st["steps"] += 1
                stale = delay * per_pl            # auctions run on the old floor while this one was on its way
                st["stale_1s"] += 1.0 * per_pl    # the same step, picked up a second later
                if d["s"] > 0:
                    gap = (d["f"] - old) * a.fill / 1000          # rupees given away per auction sold below it
                    st["below"] += stale
                    st["rs"] += stale * gap
                    st["rs_1s"] += per_pl * gap
                else:
                    st["unfilled"] += stale

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        # Start a rolling snapshot ago (every placement is in it), or at the start of the retained log.
        first = PollingStrategy.Timestamp(int((time.time() - a.replay_s) * 1e6)) if a.replay_s else PollingStrategy.First()
        while True:
            ms = await c.poll_messages(STREAM, FLOORS, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else first)
            if ms:
                apply(ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                      # more is waiting (a replay or a burst): read on at once
            await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        told = False
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                # Caught up only after a whole second in which every floor applied was inside the budget:
                # partitions interleave, so one fresh change alone does not mean the replay is finished.
                if st["sec_n"] and st["sec_age"] < budget_ns:
                    caught[0] = time.monotonic()
                    took = max(0.0, (st["last_old"] or started) - started)
                    span = f"the last {a.replay_s:g} s of the log" if a.replay_s else "the whole log"
                    print(f"[{a.name}] rebuilt from {span} in {took:.2f}s: floors for {len(table):,} of "
                          f"{total:,} placements, {st['replayed']:,} changes replayed", flush=True)
                else:
                    print(f"[{a.name}] catching up: {st['replayed']:,} changes replayed, {len(table):,} placements known",
                          flush=True)
                st["sec_n"] = st["sec_age"] = 0
                lat.flush()
                continue
            r, n = lat.flush(), st["live"]
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            stale = st["below"] + st["unfilled"]
            per_m = 1e6 * stale / a.auctions_per_s
            print(f"[{a.name}] {n:>7,} floor changes/s  floor-to-node p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  steps {st['steps']:,}  "
                  f"stale-floor auctions {stale:,.2f}/s ({per_m:.2f} per 1M): below floor {st['below']:,.2f}, "
                  f"unfilled {st['unfilled']:,.2f}, Rs {st['rs']:,.3f} given away  "
                  f"at a 1 s refresh {st['stale_1s']:,.0f}/s, Rs {st['rs_1s']:,.2f}  gaps {st['gaps']}  "
                  f"placements {len(table):,}", flush=True)
            if not told and time.monotonic() - caught[0] >= 10:
                told = True
                print(f"[{a.name}] without the log: 10 s of live changes alone would cover {len(live_seen):,} of "
                      f"{total:,} placements ({100 * len(live_seen) / total:.0f}%)", flush=True)
            st.update(live=0, steps=0, below=0.0, unfilled=0.0, rs=0.0, stale_1s=0.0, rs_1s=0.0)

    await asyncio.gather(report(), *(read(p) for p in range(PARTITIONS[FLOORS])))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="bidnode-1")
    p.add_argument("--langs", type=int, default=len(LANGS), help="same as floors.py")
    p.add_argument("--placements-per-lang", type=int, default=2000, help="same as floors.py")
    p.add_argument("--auctions-per-s", type=float, default=100_000, help="Dailyhunt auctions this node runs per second")
    p.add_argument("--fill", type=float, default=0.6, help="share of auctions that sell, for the rupee estimate")
    p.add_argument("--budget-ms", type=float, default=20, help="p99 budget for floor service to bid node")
    p.add_argument("--poll-ms", type=float, default=2, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--replay-s", type=float, default=75, help="rebuild from this many seconds back (> floors.py --snapshot-every); 0 = whole log")
    asyncio.run(main(p.parse_args()))
