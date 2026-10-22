"""REV2's side: reads the fully enriched content and runs a toy rule on the ML signals in it.

Stands in for the rules engine reading the fully enriched stream (REV2 also reads the preliminary stream
directly; that path is unchanged and not part of this kit). It reads every partition of content-enriched
directly (no consumer group), drops a repeat of a content ID it has already seen, and acts when any ML
score is at or above common.ACTION_AT (your Lua rules are your own). Prints a line a second:
  * last-signal-to-REV2 latency: from the moment the last input of a piece of content was written (by the
    enrichment service or the slowest model) to the moment REV2 has the fully enriched content, through the
    joiner and its send batches, PASS or FAIL against --budget-ms;
  * actions taken on ML signals, and time to action (content written to REV2's decision; most of it is the
    models' own inference time);
  * content that reached REV2 without one of its signals (past the joiner's wait limit), and repeats dropped.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, PollingStrategy

from common import ACTION_AT, DEFAULT_URL, ENRICHED_TOPIC, STREAM, Window, connect, ensure_all, load


async def main(a):
    admin = await connect(a.url)
    parts = await ensure_all(admin, a.partitions)
    budget_ns = a.budget_ms * 1_000_000
    seen, seen_q = set(), deque()
    lat, hop, tta = Window(), Window(), Window()
    st, total = defaultdict(int), defaultdict(int)
    caught_up = [False]
    gc.freeze()

    def apply(e, now):
        cid = e["id"]
        if cid in seen:
            total["repeats"] += 1                         # at-least-once: already evaluated
            return
        seen.add(cid)
        seen_q.append((now, cid))
        if e["miss"]:
            st["partial"] += 1
            total["partial"] += 1
        hit = [s for s, v in e["sig"].items() if v >= ACTION_AT]
        age = now - e["last"]
        if not caught_up[0]:
            st["replayed"] += 1
            if not e["miss"]:
                st["max_age"] = max(st["max_age"], age)
            return
        if not e["miss"]:
            lat.add(age / 1e6)
            hop.add((now - e["ens"]) / 1e6)
        if hit:
            st["actions"] += 1
            total["actions"] += 1
            tta.add((now - e["ns"]) / 1e6)
        st["content"] += 1

    async def reader(plist):
        c = await connect(a.url)
        consumers = {p: Consumer.Single(f"rev2-enriched-p{p}") for p in plist}
        nxt, committed = {}, {}
        while True:
            more = False
            for p in plist:
                start = PollingStrategy.Offset(nxt[p]) if p in nxt else PollingStrategy.Next()
                commit = time.monotonic() - committed.get(p, 0) >= a.commit_s
                ms = await c.poll_messages(STREAM, ENRICHED_TOPIC, consumer=consumers[p], partition_id=p,
                                           count=a.batch, auto_commit=commit, polling_strategy=start)
                if ms:
                    if commit:
                        committed[p] = time.monotonic()
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        apply(load(m.payload()), now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            now = time.time_ns()
            while seen_q and now - seen_q[0][0] > a.remember_s * 1e9:
                seen.discard(seen_q.popleft()[1])
            if not caught_up[0]:
                if st["replayed"] and st["max_age"] < budget_ns:
                    caught_up[0] = True
                print(f"[rev2] catching up: replayed {st['replayed']:,} earlier enriched content this second", flush=True)
                st.clear()
                continue
            r, h, t = lat.flush(), hop.flush(), tta.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[rev2] {st['content']:>5,} enriched/s  last-signal-to-REV2 p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  "
                  f"p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  joiner-to-REV2 p99 {h['p99']:5.1f}ms  "
                  f"actioned on ML signals {st['actions']:>3,}/s ({total['actions']:,} total)  "
                  f"time to action p50 {t['p50']:5.0f}ms  p99 {t['p99']:5.0f}ms  "
                  f"without a signal {st['partial']:>3,}/s ({total['partial']:,} total)  repeats {total['repeats']:,}",
                  flush=True)
            st.clear()

    groups = [list(range(parts))[r::a.readers] for r in range(a.readers)]
    print(f"[rev2] reading all {parts} partitions of {ENRICHED_TOPIC} on {a.readers} connection(s); "
          f"acts on any ML score >= {ACTION_AT}", flush=True)
    await asyncio.gather(report(), *(reader(g) for g in groups if g))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--partitions", type=int, default=32, help="partitions for new lane topics")
    p.add_argument("--readers", type=int, default=1, help="connections reading the enriched topic")
    p.add_argument("--budget-ms", type=float, default=100, help="p99 budget, last signal written to REV2")
    p.add_argument("--poll-ms", type=float, default=5)
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--commit-s", type=float, default=1)
    p.add_argument("--remember-s", type=float, default=120, help="how long a content ID is remembered for repeats")
    asyncio.run(main(p.parse_args()))
