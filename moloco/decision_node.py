"""A Moloco Commerce Media Decision API server: reads every item status change from the lane and keeps the
live table of which ad items may be returned.

Every server reads every partition with its own connection and its own offsets (no shared consumer group),
so adding a server never slows the others: each one gets every change, in order per item. The table is
item -> (stock, blocked); a DecideAdProducts call would check it before an item enters the auction.

Stands in for the serving path by counting decisions: the server answers --decisions-per-s decision requests,
each returning --ads-per-decision ad items spread over the items in live campaigns. Prints a line a second:
  * status-to-server latency (change made -> applied here), with PASS or FAIL against --budget-ms;
  * ads for unavailable items: ad items this server returned after the item sold out or was blocked, before
    the server knew (latency x this server's ad rate on that item), per second and per million decisions,
    with the clicks they draw at --ctr that land on a sold-out or blocked item, and what those clicks are
    charged at --cpc; next to what the same changes would cost if the server learned of every one a full
    second late ("1 s behind");
  * missed: ad slots an item could have had after a restock, before the server knew it was back;
  * gaps: a missing sequence number for an item (must stay 0).

On start it rebuilds the table by replaying the last --replay-s seconds of the log (one rolling snapshot
from status.py and the changes since); replayed messages are not timed as live.
"""
import argparse
import asyncio
import gc
import time

from apache_iggy import Consumer, PollingStrategy

from common import BACK, CATEGORIES, DEFAULT_URL, OUT, PARTITIONS, STATUS, STREAM, Window, connect, eligible, load


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, STATUS):
        print(f"[{a.name}] waiting for status.py to create the item status lane…", flush=True)
        await asyncio.sleep(1)

    total = a.cats * a.items_per_cat
    per_item = a.decisions_per_s * a.ads_per_decision / total   # ad slots per second one item gets on this server
    table = {}                                    # item -> [seq, eligible]
    lat = Window()
    st = dict(live=0, outs=0, blocks=0, backs=0, dead=0.0, missed=0.0, dead_1s=0.0, gaps=0, replayed=0,
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
                    st["gaps"] += 1               # a change for this item never arrived
            table[k] = [seq, eligible(d["n"], d["b"])]
            if caught[0] is None:
                # Still replaying: a new server first reads changes made before it started. They rebuild the
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
            if d["s"] == OUT:
                st["blocks" if d["b"] else "outs"] += 1
                st["dead"] += delay * per_item    # ad items returned for it while the change was on its way
                st["dead_1s"] += 1.0 * per_item   # the same change, learned a second later
            elif d["s"] == BACK:
                st["backs"] += 1
                st["missed"] += delay * per_item

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        # Start a rolling snapshot ago (every item is in it), or at the start of the retained log.
        first = PollingStrategy.Timestamp(int((time.time() - a.replay_s) * 1e6)) if a.replay_s else PollingStrategy.First()
        while True:
            ms = await c.poll_messages(STREAM, STATUS, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
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
                # Caught up only after a whole second in which every change applied was inside the budget:
                # partitions interleave, so one fresh change alone does not mean the replay is finished.
                if st["sec_n"] and st["sec_age"] < budget_ns:
                    caught[0] = time.monotonic()
                    took = max(0.0, (st["last_old"] or started) - started)
                    span = f"the last {a.replay_s:g} s of the log" if a.replay_s else "the whole log"
                    off = sum(1 for v in table.values() if not v[1])
                    print(f"[{a.name}] rebuilt from {span} in {took:.2f}s: status for {len(table):,} of "
                          f"{total:,} items ({off:,} not eligible), {st['replayed']:,} changes replayed", flush=True)
                else:
                    print(f"[{a.name}] catching up: {st['replayed']:,} changes replayed, {len(table):,} items known",
                          flush=True)
                st["sec_n"] = st["sec_age"] = 0
                lat.flush()
                continue
            r, n = lat.flush(), st["live"]
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            per_m = 1e6 * st["dead"] / a.decisions_per_s
            clicks = st["dead"] * a.ctr
            print(f"[{a.name}] {n:>7,} status changes/s  status-to-server p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  stock-outs {st['outs']:,}  "
                  f"blocks {st['blocks']:,}  restocks {st['backs']:,}  "
                  f"ads for unavailable items {st['dead']:,.3f}/s ({per_m:.3f} per 1M decisions), "
                  f"clicks {clicks:,.5f}, ${clicks * a.cpc:,.5f} charged  missed {st['missed']:,.3f}  "
                  f"1 s behind {st['dead_1s']:,.1f}/s, ${st['dead_1s'] * a.ctr * a.cpc:,.3f}  gaps {st['gaps']}  "
                  f"items {len(table):,}", flush=True)
            if not told and time.monotonic() - caught[0] >= 10:
                told = True
                print(f"[{a.name}] without the log: 10 s of live changes alone would cover {len(live_seen):,} of "
                      f"{total:,} items ({100 * len(live_seen) / total:.0f}%)", flush=True)
            st.update(live=0, outs=0, blocks=0, backs=0, dead=0.0, missed=0.0, dead_1s=0.0)

    await asyncio.gather(report(), *(read(p) for p in range(PARTITIONS[STATUS])))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="dapi-1")
    p.add_argument("--cats", type=int, default=len(CATEGORIES), help="same as status.py")
    p.add_argument("--items-per-cat", type=int, default=12500, help="same as status.py")
    p.add_argument("--decisions-per-s", type=float, default=2000, help="decision requests this server answers per second")
    p.add_argument("--ads-per-decision", type=float, default=10, help="ad items returned per decision")
    p.add_argument("--ctr", type=float, default=0.01, help="click-through rate, for the clicks estimate")
    p.add_argument("--cpc", type=float, default=0.60, help="cost per click in USD, for what those clicks are charged")
    p.add_argument("--budget-ms", type=float, default=20, help="p99 budget for status service to Decision API server")
    p.add_argument("--poll-ms", type=float, default=2, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--replay-s", type=float, default=75, help="rebuild from this many seconds back (> status.py --snapshot-every); 0 = whole log")
    asyncio.run(main(p.parse_args()))
