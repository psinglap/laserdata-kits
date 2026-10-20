"""A decision worker: owns a fixed set of partitions of the shopping-event topic and decides, per
subscriber, whether this event should trigger a text.

Worker --worker of --workers owns every partition p with p % workers == worker and reads each one
directly (no consumer group, no rebalance), in order. Because the storefront keys events by
subscriber, every event of a subscriber reaches this one worker, one after another: the decision
for a subscriber is never made twice at the same time, and no lock is needed. Offsets are stored
under a per-partition consumer name, so whichever worker owns a partition next resumes from it.

The decision rule is deliberately small (the real engine is Attentive's): an add_to_cart or
checkout_started event triggers one reminder text, unless this subscriber already got one, or
purchased, within --cooldown-s. Each text is written to the send-intents topic, keyed by
subscriber, with this worker's ID, for the send service.

Prints a line a second:
  * event-to-decision latency (written by the storefront to decided here), PASS or FAIL against --budget-ms;
  * texts queued, and repeat triggers held back by the per-subscriber state;
  * order violations, missing events, and subscribers whose events came in on another partition
    (all three are 0 when events are keyed by subscriber).
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy, SendMessage, Partitioning

from common import (DEFAULT_URL, EVENTS_TOPIC, INTENT, SENDS_TOPIC, STREAM, Window, connect, ensure_topic, load,
                    msg, owner, part)


async def main(a):
    name = f"decide-{a.worker}"
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, EVENTS_TOPIC, a.partitions)     # whoever starts first creates the topics
    await ensure_topic(admin, STREAM, SENDS_TOPIC, a.send_partitions)
    parts = (await admin.get_topic(STREAM, EVENTS_TOPIC)).partitions_count
    send_parts = (await admin.get_topic(STREAM, SENDS_TOPIC)).partitions_count
    mine = [p for p in range(parts) if owner(p, a.workers) == a.worker]
    budget_ns = a.budget_ms * 1_000_000
    cool_ns = int(a.cooldown_s * 1e9)
    last_seq, texted, bought, seen_on = {}, {}, {}, {}
    lat = Window()
    st, total = defaultdict(int), defaultdict(int)
    caught_up = [False]                                   # a new reader first replays what was written before it
    gc.freeze()                                           # keep start-up objects out of the collector's way

    def decide(e, now, out):
        sub, seq = e["u"], e["seq"]
        if "x" not in e:                                  # keyed: one partition per subscriber, in order
            if seen_on.setdefault(sub, e["_p"]) != e["_p"]:
                total["moved"] += 1
            last = last_seq.get(sub, 0)
            if seq <= last:
                total["out_of_order" if seq < last else "repeats"] += 1
                return
            if seq > last + 1 and last:
                total["lost"] += seq - last - 1
            last_seq[sub] = seq
        ev = e["e"]
        if ev == "purchase":
            bought[sub] = e["ns"]
        elif ev in INTENT:
            if e["ns"] - texted.get(sub, -cool_ns) < cool_ns or e["ns"] - bought.get(sub, -cool_ns) < cool_ns:
                st["held"] += 1                           # this subscriber already has a text, or just bought
            else:
                texted[sub] = e["ns"]
                kind = "checkout_reminder" if ev == "checkout_started" else "cart_reminder"
                out[part(sub, send_parts)].append(SendMessage(msg(
                    ns=e["ns"], dns=now, u=sub, b=e["b"], kind=kind, w=a.worker, seq=seq)))
                st["texts"] += 1
        age = now - e["ns"]
        if not caught_up[0]:
            st["replayed"] += 1                           # written before this worker started: not live latency
            st["max_age"] = max(st["max_age"], age)
        else:
            lat.add(age / 1e6)
        st["events"] += 1

    async def reader(plist):
        c, out_c = await connect(a.url), await connect(a.url)   # one connection to read, one to write
        consumers = {p: Consumer.Single(f"decision-p{p}") for p in plist}
        nxt = {}
        while True:
            more = False
            out = defaultdict(list)
            for p in plist:
                start = PollingStrategy.Offset(nxt[p]) if p in nxt else PollingStrategy.Next()
                ms = await c.poll_messages(STREAM, EVENTS_TOPIC, consumer=consumers[p], partition_id=p,
                                           count=a.batch, auto_commit=True, polling_strategy=start)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        e = load(m.payload())
                        e["_p"] = p
                        decide(e, now, out)
                    more = more or len(ms) == a.batch
            if out:
                await asyncio.gather(*(out_c.send_messages(STREAM, SENDS_TOPIC, Partitioning.partition_id(pid), ms)
                                       for pid, ms in out.items()))
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

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
            print(f"[{name}] {r['n']:>6,} events/s decided  event-to-decision p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"texts {st['texts']:>5,}/s  repeat triggers held {st['held']:>5,}/s  "
                  f"out of order {total['out_of_order']:,}  missing {total['lost']:,}  "
                  f"on another partition {total['moved']:,}  subscribers {len(last_seq) or len(texted):,}", flush=True)
            st.clear()

    groups = [mine[r::a.readers] for r in range(a.readers)]
    print(f"[{name}] owns partitions {mine} of {parts} (worker {a.worker} of {a.workers}), "
          f"reading on {a.readers} connections", flush=True)
    await asyncio.gather(report(), *(reader(g) for g in groups if g))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--worker", type=int, default=0)
    p.add_argument("--workers", type=int, default=4, help="decision workers splitting the partitions")
    p.add_argument("--partitions", type=int, default=64, help="partitions for a new events topic")
    p.add_argument("--send-partitions", type=int, default=16, help="partitions for a new send-intents topic")
    p.add_argument("--readers", type=int, default=2, help="connections reading this worker's partitions")
    p.add_argument("--cooldown-s", type=float, default=3600, help="at most one reminder per subscriber in this window")
    p.add_argument("--budget-ms", type=float, default=50, help="p99 budget, event written to decision made")
    p.add_argument("--poll-ms", type=float, default=2, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
