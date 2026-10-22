"""A campaign reporting writer for one status type, reading only that status's topic.

Reads every partition of its status topic directly (its own offsets, no consumer group) on
--readers connections. For each webhook it drops provider retries (a message ID already
counted for this status), times it, and adds it to its campaign's count. Every --flush-ms it
writes one row per changed campaign to the report store (a dict here; your reporting database
in production), so thousands of webhooks a second become a few row writes a second.

--max-rate caps how many webhooks a second this writer takes, to protect the database.
Delivered runs uncapped by default, with its own readers. Read runs capped: when a surge
exceeds the cap, the rest waits on status-read, in the log, and drains at the cap.

Prints a line a second:
  * webhook-to-reporter latency, PASS or FAIL against --budget-ms;
  * report rows written, and the share of statuses in the report within 1 s of the webhook;
  * provider retries dropped (each status counted once per message);
  * when held at its cap: rate taken, backlog on its own topic, oldest webhook waiting.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, STATUSES, STREAM, Seen, Window, connect, ensure_topic, load

BANK = 0.5                                        # seconds of cap a partition may bank for a burst


async def main(a):
    name = a.status
    topic, parts = STATUSES[a.status]
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, topic, parts)
    plist = list(range(parts))
    capped = a.max_rate > 0
    per_part = a.max_rate / parts if capped else 0
    tokens = {p: per_part * BANK for p in plist}
    refilled = {p: time.monotonic() for p in plist}
    nxt = {}                                      # next offset per partition
    seen = Seen()
    budget_ms = a.budget_ms
    lat, held_wait = Window(), 0.0
    st = defaultdict(int)
    counts = defaultdict(int)                     # the report store: campaign -> statuses counted
    pending = defaultdict(int)                    # counted since the last flush, per campaign
    oldest = []                                   # webhook times not yet in the report
    caught_up = False
    held_at, first_held = 0, None
    gc.freeze()                                   # keep startup tables out of the collector's way

    def apply(ms, now):
        nonlocal held_wait
        for m in ms:
            e = load(m.payload())
            if not seen.add(e["m"]):
                st["retries"] += 1                # the provider sent this webhook again: count it once
                continue
            age = (now - e["ns"]) / 1e6
            pending[e["c"]] += 1
            oldest.append(e["ns"])
            if now - held_at < 2e9:
                st["held"] += 1
                held_wait = max(held_wait, age / 1000)
            else:
                lat.add(age)
                st["live"] += 1

    async def reader(mine):
        nonlocal held_at, first_held
        c = await connect(a.url)
        consumers = {p: Consumer.Single(f"report-{name}-{p}") for p in mine}
        while True:
            more = False
            for p in mine:
                allow = a.batch
                if capped:
                    now = time.monotonic()
                    tokens[p] = min(per_part * BANK, tokens[p] + (now - refilled[p]) * per_part)
                    refilled[p] = now
                    allow = min(a.batch, int(tokens[p]))
                    if allow < 1:
                        continue
                start = PollingStrategy.Offset(nxt[p]) if p in nxt else PollingStrategy.First()
                ms = await c.poll_messages(STREAM, topic, consumer=consumers[p], partition_id=p,
                                           count=allow, auto_commit=False, polling_strategy=start)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    if capped:
                        tokens[p] -= len(ms)
                        if len(ms) == allow < a.batch:       # the cap, not the topic, ended this read:
                            held_at = time.time_ns()         # the rest waits on the status topic
                            if first_held is None:
                                first_held = time.monotonic()
                    apply(ms, time.time_ns())
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def flush():
        next_t = time.monotonic()
        while True:
            next_t += a.flush_ms / 1000                       # from its due time, so a slow flush never drifts
            await asyncio.sleep(max(0.0, next_t - time.monotonic()))
            if not pending:
                continue
            now = time.time_ns()
            for c, k in pending.items():                      # one row write per changed campaign
                counts[c] += k
            st["rows"] += len(pending)
            st["in_report"] += len(oldest)
            st["in_report_1s"] += sum(1 for ns in oldest if now - ns <= 1e9)
            st["freshness"] = max(st["freshness"], (now - min(oldest)) / 1e6)
            pending.clear()
            oldest.clear()

    async def backlog():
        t = await admin.get_topic(STREAM, topic)
        return max(0, t.messages_count - sum(nxt.values())) if t else 0

    async def report():
        nonlocal caught_up, held_wait, first_held
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            stamp = int(time.time())
            if not caught_up:
                # Live only after a whole second in which every status read was inside the budget:
                # earlier webhooks still on the topic are replayed first, and are not live latency.
                if r["n"] and r["max"] < budget_ms:
                    caught_up = True
                print(f"[{name}] catching up: {r['n']:,} earlier webhooks replayed, worst age {r['max']:,.0f} ms"
                      if r["n"] else f"[{name}] waiting for {topic}", flush=True)
                st.clear()
                continue
            ok = "PASS" if r["p99"] < budget_ms else "FAIL"
            within = 100 * st["in_report_1s"] / st["in_report"] if st["in_report"] else 100.0
            print(f"[{name}] {st['live'] + st['held']:>6,} {name} statuses/s  webhook-to-reporter p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{budget_ms:g}ms)  report rows {st['rows']:,}/s  "
                  f"in report within 1 s {within:6.2f}%  retries dropped {st['retries']:,}  "
                  f"campaigns {len(counts)}  total {sum(counts.values()):,}  t={stamp}", flush=True)
            if first_held is not None:
                b = await backlog()
                if b > a.batch or (time.time_ns() - held_at < 2e9 and st["held"]):
                    print(f"[{name}] held at its write cap: took {st['held']:,}/s (cap {a.max_rate:,.0f}/s)  "
                          f"backlog {b:,} on {topic}  oldest waiting {held_wait:5.1f} s  t={stamp}", flush=True)
                else:
                    print(f"[{name}] back inside its write cap: backlog on {topic} cleared "
                          f"{time.monotonic() - first_held:.1f} s after it was first held  t={stamp}", flush=True)
                    first_held = None
            held_wait = 0.0
            st.clear()

    groups = [plist[r::a.readers] for r in range(a.readers)]
    cap = f"capped at {a.max_rate:,.0f} webhooks/s" if capped else "uncapped"
    print(f"[{name}] reading {topic}, {parts} partitions, on {a.readers} connections, {cap}", flush=True)
    await asyncio.gather(report(), flush(), *(reader(g) for g in groups if g))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--status", choices=list(STATUSES), default="delivered")
    p.add_argument("--readers", type=int, default=2, help="connections reading this status topic")
    p.add_argument("--max-rate", type=float, default=0, help="webhooks a second this writer takes (0 = uncapped)")
    p.add_argument("--budget-ms", type=float, default=100, help="p99 budget, webhook to reporter")
    p.add_argument("--flush-ms", type=float, default=250, help="how often changed campaign rows are written")
    p.add_argument("--poll-ms", type=float, default=5, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
