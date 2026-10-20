"""A traffic-shaping node: reads every appetite change from the lane and keeps the live table it routes by.

Every node reads every partition with its own connection and its own offsets (no shared consumer
group), so adding a node never slows the others: each one gets every change, in order per pair.
The table is (DSP, supply segment) -> recent bid rate and whether the DSP is bidding; your shaping
model would read it next to its own features when it decides which DSPs get a request.

Stands in for the request path by counting callouts: the node receives --requests-per-s ad requests
spread over the supply segments, and sends each one to every DSP the table says is bidding on its
segment. Prints a line a second:
  * exchange-to-shaper latency (change seen at the exchange -> applied here), with PASS or FAIL
    against --budget-ms;
  * wasted callouts: requests sent to a DSP after it stopped bidding on that segment, before this
    node knew (per second, and per million callouts), next to what the same flips would waste if the
    table refreshed once a second;
  * bids held back: requests not sent to a DSP that had started bidding again, before this node knew,
    times its bid rate;
  * gaps: a missing sequence number for a pair (must stay 0).

On start it rebuilds the table by replaying the last --replay-s seconds of the log (one rolling
snapshot from exchange.py and the changes since); replayed messages are not timed as live.
"""
import argparse
import asyncio
import gc
import time

from apache_iggy import Consumer, PollingStrategy

from common import APPETITE, DEFAULT_URL, PARTITIONS, STREAM, Window, connect, load


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, APPETITE):
        print(f"[{a.name}] waiting for exchange.py to create the appetite lane…", flush=True)
        await asyncio.sleep(1)

    total = a.dsps * a.segments
    per_pair = a.requests_per_s / a.segments      # callouts per second to one DSP on one segment, while it bids
    table = {}                                    # pair -> [seq, bid rate, bidding?]
    lat = Window()
    st = dict(live=0, flips=0, wasted=0.0, wasted_1s=0.0, held=0.0, gaps=0, replayed=0, sec_n=0, sec_age=0, last_old=0.0)
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
                    st["gaps"] += 1               # a change for this pair never arrived
            table[k] = [seq, d["r"], d["on"]]
            if caught[0] is None:
                # Still replaying: a new node first reads changes made before it started. They rebuild the
                # table but are not live latency. Track this second's oldest one to know when replay is over.
                st["replayed"] += 1
                st["sec_n"] += 1
                st["sec_age"] = max(st["sec_age"], now - ns)
                if now - ns >= budget_ns:
                    st["last_old"] = time.monotonic()   # when the replay last applied a change from before the start
                continue
            if seq == 1:
                continue                          # the start-up snapshot is not a live change
            delay = (now - ns) / 1e9
            lat.add(delay * 1e3)
            st["live"] += 1
            live_seen.add(k)
            if d["f"]:
                st["flips"] += 1
                if d["on"]:
                    st["held"] += delay * per_pair * d["r"]     # requests this node held back from a DSP bidding again
                else:
                    st["wasted"] += delay * per_pair            # requests it kept sending to a DSP that had stopped
                    st["wasted_1s"] += 1.0 * per_pair           # the same flip, picked up a second later

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        # Start a rolling snapshot ago (every pair is in it), or at the start of the retained log.
        first = PollingStrategy.Timestamp(int((time.time() - a.replay_s) * 1e6)) if a.replay_s else PollingStrategy.First()
        while True:
            ms = await c.poll_messages(STREAM, APPETITE, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
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
                    print(f"[{a.name}] rebuilt from {span} in {took:.2f}s: appetite for {len(table):,} of "
                          f"{total:,} pairs, {st['replayed']:,} changes replayed", flush=True)
                else:
                    print(f"[{a.name}] catching up: {st['replayed']:,} changes replayed, {len(table):,} pairs known",
                          flush=True)
                st["sec_n"] = st["sec_age"] = 0
                lat.flush()
                continue
            r, n = lat.flush(), st["live"]
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            bidding = sum(1 for v in table.values() if v[2])
            callouts = bidding * per_pair
            per_m = 1e6 * st["wasted"] / callouts if callouts else 0.0
            print(f"[{a.name}] {n:>7,} changes/s  exchange-to-shaper p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  flips {st['flips']:,}  "
                  f"wasted callouts {st['wasted']:,.1f}/s ({per_m:.2f} per 1M)  at a 1 s refresh {st['wasted_1s']:,.0f}/s  "
                  f"bids held back {st['held']:,.2f}/s  gaps {st['gaps']}  pairs {len(table):,}", flush=True)
            if not told and time.monotonic() - caught[0] >= 10:
                told = True
                print(f"[{a.name}] without the log: 10 s of live changes alone would cover {len(live_seen):,} of "
                      f"{total:,} pairs ({100 * len(live_seen) / total:.0f}%)", flush=True)
            st.update(live=0, flips=0, wasted=0.0, wasted_1s=0.0, held=0.0)

    await asyncio.gather(report(), *(read(p) for p in range(PARTITIONS[APPETITE])))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="shaper-1")
    p.add_argument("--dsps", type=int, default=60, help="same as exchange.py")
    p.add_argument("--segments", type=int, default=5000, help="same as exchange.py")
    p.add_argument("--requests-per-s", type=float, default=200_000, help="ad requests this node shapes per second")
    p.add_argument("--budget-ms", type=float, default=20, help="p99 budget for exchange-to-shaper")
    p.add_argument("--poll-ms", type=float, default=2, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--replay-s", type=float, default=75, help="rebuild from this many seconds back (> exchange.py --snapshot-every); 0 = whole log")
    asyncio.run(main(p.parse_args()))
