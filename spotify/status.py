"""The episode status service: it reads every stage change from the lane, keeps each episode's status for the
creator, and watches the publishing queues.

  * Receipt: an upload's "received and queued" is in the view milliseconds after the upload API wrote it, so the
    creator sees it before they wonder whether the upload worked.
  * Re-upload: a re-upload of a file already in the pipeline is answered "already received, queued since ..."
    from the view, instead of being queued and transcoded a second time. An answer is wrong if the view did not
    know the episode yet (it would have been queued again).
  * Backlog watch: every --watch-ms the oldest waiting episode in each priority queue is read off the view. When
    the oldest new episode has waited longer than --threshold-s the watch raises an alert (and clears it below
    half the line). Each alert prints how long after the real crossing it was raised.

The service reads every partition directly (its own offsets, no consumer group) on its own connection. On start
it rebuilds the view from the log before it answers anything, so a new replica or a restarted one comes up with
every episode's status and the queues as they are.

Prints a line a second: stage-change-to-view latency (published -> applied here), PASS or FAIL against
--budget-ms, receipts and their latency, re-uploads answered and wrong answers, the queues, repeats dropped,
sequence gaps.
"""
import argparse
import asyncio
import gc
import heapq
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, NEW, PARTITIONS, STATUS, STREAM, UPDATE, Window, connect, ensure_topics, load


async def main(a):
    await ensure_topics(await connect(a.url), STATUS)
    reader = await connect(a.url)
    P = PARTITIONS[STATUS]
    start_ns = time.time_ns()
    view = {}                                 # episode -> [stage, priority, queued ns, n]
    waiting = {NEW: [], UPDATE: []}           # heaps of (queued ns, episode); entries past "received" are skipped
    lat, rec = Window(), Window()
    st, run = defaultdict(int), defaultdict(int)
    live, caught, caught_ns = set(), [None], [0]
    alert = [None]
    gc.freeze()

    def apply(e, live_now):
        k = e["e"]
        r = view.get(k)
        if r is not None and e["n"] <= r[3]:
            run["repeats"] += 1               # same event number seen before (redelivery, replay): apply it once
            return False
        if r is not None and e["n"] > r[3] + 1:
            run["gaps"] += 1                  # a stage change of this episode never arrived
        if e["k"] == "reupload":
            if live_now:
                if r is not None:
                    st["answered"] += 1       # "already received, queued since ...": not queued again
                else:
                    st["wrong"] += 1          # the view did not know it yet: it would have been queued again
        elif e["st"] == 0 and r is not None and live_now:
            st["wrong"] += 1                  # a new upload the view thought it already had
        if r is None:
            r = view[k] = [0, e["pr"], e["q"], 0]
        r[0], r[3] = e["st"], e["n"]
        if e["k"] == "stage" and e["st"] == 0:
            heapq.heappush(waiting[e["pr"]], (e["q"], k))
        return True

    def oldest(pr, now_ns):
        h = waiting[pr]
        while h and view[h[0][1]][0] != 0:    # started since it was queued
            heapq.heappop(h)
        return ((now_ns - h[0][0]) / 1e9, h[0][0]) if h else (0.0, None)

    def depth(pr):
        return sum(1 for _, k in waiting[pr] if view[k][0] == 0)

    async def read():
        who = {p: Consumer.Single(f"{a.name}-{p}") for p in range(P)}
        nxt = {}
        while True:
            more = False
            for p in range(P):
                ms = await reader.poll_messages(STREAM, STATUS, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                                else PollingStrategy.First())
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        e = load(m.payload())
                        if caught[0] is None or e["ns"] < start_ns:
                            run["replayed"] += 1   # rebuilding: not live latency
                            apply(e, False)
                        elif apply(e, True):
                            ms_ = (now - e["ns"]) / 1e6
                            lat.add(ms_)
                            if e["k"] == "stage" and e["st"] == 0:
                                rec.add(ms_)
                                st["receipts"] += 1
                                st["receipt_1s"] += ms_ < 1000
                if len(ms) == a.batch:
                    more = True               # a replay or a burst: read on at once
                elif p not in live:
                    live.add(p)               # this partition has been read to the end of the log once
                    if len(live) == P:
                        now_ns = time.time_ns()
                        caught[0], caught_ns[0] = time.monotonic(), now_ns
                        print(f"[{a.name}] rebuilt the status view from the log in {(now_ns - start_ns) / 1e9:.2f}s: "
                              f"{len(view):,} episodes, {run['replayed']:,} stage changes; waiting new {depth(NEW):,} "
                              f"(oldest {oldest(NEW, now_ns)[0]:.1f}s) updates {depth(UPDATE):,}", flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def watch():
        while caught[0] is None:
            await asyncio.sleep(0.05)
        due = time.monotonic()
        while True:
            now_ns = time.time_ns()
            w, q = oldest(NEW, now_ns)
            if alert[0] is None and w > a.threshold_s:
                cross = q / 1e9 + a.threshold_s
                late = now_ns / 1e9 - cross
                on_start = cross * 1e9 < caught_ns[0]     # crossed before this replica had rebuilt its view
                alert[0] = now_ns
                print(f"[watch] ALERT new episodes waiting: oldest {w:.2f}s over the {a.threshold_s:g}s line, "
                      f"{depth(NEW):,} waiting; crossed at {cross:.3f}, raised at {now_ns / 1e9:.3f}, "
                      + ("already over when this replica came up" if on_start else f"{late * 1000:.0f} ms after it crossed"),
                      flush=True)
            elif alert[0] is not None and w < a.threshold_s / 2:
                print(f"[watch] clear: oldest new episode {w:.2f}s, under {a.threshold_s / 2:g}s at {now_ns / 1e9:.3f}, "
                      f"alert open {(now_ns - alert[0]) / 1e9:.1f}s", flush=True)
                alert[0] = None
            due += a.watch_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the status view from the log: {run['replayed']:,} stage changes so far",
                      flush=True)
                lat.flush()
                rec.flush()
                continue
            r, c = lat.flush(), rec.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            now_ns = time.time_ns()
            wn, wu = oldest(NEW, now_ns)[0], oldest(UPDATE, now_ns)[0]
            within = 100 * st["receipt_1s"] / st["receipts"] if st["receipts"] else 100.0
            print(f"[{a.name}] stage changes {r['n']:>5,}/s  stage-change-to-view p50 {r['p50']:5.1f}ms  "
                  f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"receipts {st['receipts']:,}/s (p99 {c['p99']:.1f}ms, within 1 s {within:.1f}%)  "
                  f"re-uploads answered {st['answered']:,} wrong {st['wrong']}  "
                  f"waiting new {depth(NEW):,} (oldest {wn:.1f}s) updates {depth(UPDATE):,} (oldest {wu:.1f}s)  "
                  f"{'ALERT  ' if alert[0] else ''}repeats dropped {run['repeats']:,}  gaps {run['gaps']}", flush=True)
            st.clear()

    await asyncio.gather(read(), watch(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="status-1")
    p.add_argument("--budget-ms", type=float, default=50, help="stage-change-to-view p99 budget")
    p.add_argument("--threshold-s", type=float, default=300, help="alert when the oldest new episode waits longer")
    p.add_argument("--watch-ms", type=float, default=100, help="how often the watch reads the queues")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
