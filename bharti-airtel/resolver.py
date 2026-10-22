"""An enforcement point: reads every verdict from the lane and keeps the live verdict table it resolves
(or scans) links against. Stands in for a DNS resolver node of the Fraud Detection Solution, or the SMS link
scanner; run one per node.

Every node reads every partition with its own connection and its own offsets (no shared consumer group), so
adding a node never slows the others: each one gets every verdict, in order per domain. The table is
domain -> block or clean; the resolver would read it on each lookup and send a blocked domain to the
warning page.

Stands in for the lookup path by counting lookups. Prints a line a second:
  * verdict-to-resolver latency (verdict set -> applied here), with PASS or FAIL against --budget-ms;
  * clicks let through: lookups this node resolved on a domain after it was flipped to block, before the
    node had the block (latency x --clicks-per-s, the clicks a just-flagged smishing link gets on one node),
    per second and per million lookups; next to the clicks the same blocks would let through if this node
    kept its cached clean verdict until the daily database refresh (--campaign-life-s of clicks each);
    both judged against this node's verdict before the flip was applied;
  * legit lookups still blocked: lookups on a domain after its block was lifted, before the node had it;
  * gaps: a missing sequence number for a domain (must stay 0).

On start it rebuilds the table by replaying the last --replay-s seconds of the log (one rolling snapshot
from verdicts.py and the verdicts since); replayed messages are not timed as live.
"""
import argparse
import asyncio
import gc
import time

from apache_iggy import Consumer, PollingStrategy

from common import BLOCK, CLEAN, DEFAULT_URL, PARTITIONS, STREAM, VERDICTS, Window, connect, load


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, VERDICTS):
        print(f"[{a.name}] waiting for verdicts.py to create the verdict lane…", flush=True)
        await asyncio.sleep(1)

    table = {}                                    # domain -> [seq, verdict]
    lat = Window()
    st = dict(live=0, blocks=0, lifted=0, through=0.0, daily=0.0, wrong=0.0, gaps=0, replayed=0,
              sec_n=0, sec_age=0, last_old=0.0)
    started, caught = time.monotonic(), [None]
    budget_ns = a.budget_ms * 1_000_000
    gc.freeze()                                   # keep start-up objects out of the collector's way

    def apply(ms, now):
        for m in ms:
            d = load(m.payload())
            k, seq, ns, v = d["k"], d["q"], d["ns"], d["v"]
            prev = table.get(k)
            if prev is not None:
                if seq <= prev[0]:
                    continue                      # a repeat (rolling snapshot or at-least-once redelivery): skip it
                if seq > prev[0] + 1:
                    st["gaps"] += 1               # a verdict for this domain never arrived
            old = prev[1] if prev is not None else None   # this node's verdict before this one is applied
            table[k] = [seq, v]
            if caught[0] is None:
                # Still replaying: a new node first reads verdicts set before it started. They rebuild the
                # table but are not live latency. Track this second's oldest one to know when replay is over.
                st["replayed"] += 1
                st["sec_n"] += 1
                st["sec_age"] = max(st["sec_age"], now - ns)
                if now - ns >= budget_ns:
                    st["last_old"] = time.monotonic()
                continue
            if seq == 1:
                continue                          # the start-up snapshot is not a live verdict
            delay = (now - ns) / 1e9
            lat.add(delay * 1e3)
            st["live"] += 1
            # Both counts are judged against the same state: this node's verdict before the flip was applied.
            if old == CLEAN and v == BLOCK:
                st["blocks"] += 1
                st["through"] += delay * a.clicks_per_s           # clicks resolved while the block was on its way
                st["daily"] += a.campaign_life_s * a.clicks_per_s  # the same block, held back to the daily refresh
            elif old == BLOCK and v == CLEAN:
                st["lifted"] += 1
                st["wrong"] += delay * a.legit_per_s

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        # Start a rolling snapshot ago (every domain is in it), or at the start of the retained log.
        first = PollingStrategy.Timestamp(int((time.time() - a.replay_s) * 1e6)) if a.replay_s else PollingStrategy.First()
        while True:
            ms = await c.poll_messages(STREAM, VERDICTS, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else first)
            if ms:
                apply(ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                      # more is waiting (a replay or a burst): read on at once
            await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                # Caught up only after a whole second in which every verdict applied was inside the budget:
                # partitions interleave, so one fresh verdict alone does not mean the replay is finished.
                if st["sec_n"] and st["sec_age"] < budget_ns:
                    caught[0] = time.monotonic()
                    took = max(0.0, (st["last_old"] or started) - started)
                    span = f"the last {a.replay_s:g} s of the log" if a.replay_s else "the whole log"
                    blocked = sum(1 for e in table.values() if e[1] == BLOCK)
                    print(f"[{a.name}] rebuilt from {span} in {took:.2f}s: verdicts for {len(table):,} of "
                          f"{a.domains:,} domains ({blocked:,} blocked), {st['replayed']:,} verdicts replayed", flush=True)
                else:
                    print(f"[{a.name}] catching up: {st['replayed']:,} verdicts replayed, {len(table):,} domains known",
                          flush=True)
                st["sec_n"] = st["sec_age"] = 0
                lat.flush()
                continue
            r, n = lat.flush(), st["live"]
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            per_m = 1e6 * st["through"] / a.lookups_per_s
            print(f"[{a.name}] {n:>7,} verdicts/s  verdict-to-resolver p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  to block {st['blocks']:,}  "
                  f"clicks let through {st['through']:,.3f}/s ({per_m:.4f} per 1M lookups)  "
                  f"at the daily refresh {st['daily']:,.0f}  lifted {st['lifted']:,}  "
                  f"legit lookups still blocked {st['wrong']:,.3f}/s  gaps {st['gaps']}  domains {len(table):,}", flush=True)
            st.update(live=0, blocks=0, lifted=0, through=0.0, daily=0.0, wrong=0.0)

    await asyncio.gather(report(), *(read(p) for p in range(PARTITIONS[VERDICTS])))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="resolver-haryana")
    p.add_argument("--domains", type=int, default=500_000, help="same as verdicts.py")
    p.add_argument("--lookups-per-s", type=float, default=200_000, help="DNS lookups this node resolves per second")
    p.add_argument("--clicks-per-s", type=float, default=2, help="clicks per second on a just-flagged smishing link, on this node")
    p.add_argument("--legit-per-s", type=float, default=5, help="lookups per second on a domain whose block was just lifted")
    p.add_argument("--campaign-life-s", type=float, default=3600, help="seconds a smishing link keeps getting clicks")
    p.add_argument("--budget-ms", type=float, default=25, help="p99 budget for verdict to resolver")
    p.add_argument("--poll-ms", type=float, default=2, help="wait between polls of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--replay-s", type=float, default=75, help="rebuild from this many seconds back (> verdicts.py --snapshot-every); 0 = whole log")
    asyncio.run(main(p.parse_args()))
