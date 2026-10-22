"""The signals joiner: joins every ML signal to the content it belongs to and writes the fully enriched
content the moment the last expected signal arrives.

Joiner --joiner of --joiners owns every partition number p with p % joiners == joiner, of every input
topic (content-preliminary and each signal-<name> topic), and reads those partitions directly, in turn on
one connection (--readers splits them across more), no consumer group, no rebalance. Content and its signals are keyed by
content ID with the same partition function, so this joiner sees a piece of content and all of its
signals. Per content ID it keeps the signals that have arrived; when the last expected one lands, it
writes the fully enriched content to content-enriched (keyed by content ID) from a separate sender task
every --flush-ms. Content still missing a signal --max-wait-s after it was written goes out without it
(a left join, as REV2 must still see it); a signal that arrives after that is counted as late. A signal
that arrives before its content is held until the content comes. A repeat (the same content, or the same
signal for a content ID) is dropped, so at-least-once delivery doesn't double anything.

Prints a line a second:
  * last-signal-to-joined latency: from the moment the last input of a piece of content was written to
    the moment it was joined here, PASS or FAIL against --budget-ms;
  * join-rate: signals joined / signals expected, over all content written out so far (Reddit's own
    join-rate metric), and the same events under a plain --compare-window-s event-time tumbling window,
    where a signal only joins if it was written before its content's window closed (for comparison only);
  * content to fully enriched this second (dominated by the models' own inference time), and the median
    over recent content of when that window would have written it out (at window close);
  * content written out without a signal, late signals, repeats dropped.
"""
import argparse
import asyncio
import gc
import heapq
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, ENRICHED_TOPIC, PRELIM_TOPIC, SIGNALS, STREAM, Window, connect, ensure_all, load,
                    msg, owner, part, signal_topic)


async def main(a):
    name = f"joiner-{a.joiner}"
    admin = await connect(a.url)
    parts = await ensure_all(admin, a.partitions)          # whoever starts first creates the topics
    mine = [p for p in range(parts) if owner(p, a.joiners) == a.joiner]
    budget_ns = a.budget_ms * 1_000_000
    wait_ns = int(a.max_wait_s * 1e9)
    win_ns = int(a.compare_window_s * 1e9)
    pend, early, done = {}, {}, {}                         # content ID -> waiting content / early signals / emit time
    early_q, done_q, deadlines = deque(), deque(), []
    lat, wait = Window(), Window()
    win_wait = deque(maxlen=20_000)                       # recent window write-out delays (they vary by second)
    st, total = defaultdict(int), defaultdict(int)
    caught_up = [False]                                   # a new reader first replays what was written before it
    out = defaultdict(list)                               # enriched content waiting for the next flush
    gc.freeze()                                           # keep start-up objects out of the collector's way

    def emit(cid, r, now):
        got = r["got"]
        miss = [s for s in r["exp"] if s not in got]
        out[part(cid, parts)].append(SendMessage(msg(
            ns=r["ns"], id=cid, last=r["last"], ens=now, kind=r["kind"], sr=r["sr"],
            sig={s: v[0] for s, v in got.items()}, miss=miss)))
        del pend[cid]
        done[cid] = now
        done_q.append((now, cid))
        close = (r["ns"] // win_ns + 1) * win_ns          # the content's window closes here
        total["expected"] += len(r["exp"])                # join-rates are counted over the whole run
        total["joined"] += len(got)
        total["win_joined"] += sum(1 for _, ns in got.values() if ns < close)
        if miss:
            st["partial"] += 1
            return
        age = now - r["last"]
        if not caught_up[0]:
            st["replayed"] += 1                           # written before this joiner started: not live latency
            st["max_age"] = max(st["max_age"], age)
            return
        lat.add(age / 1e6)
        wait.add((now - r["ns"]) / 1e6)
        win_wait.append((close - r["ns"]) / 1e6)          # the window writes it out at close, joined or not
        st["complete"] += 1

    def on_content(e, now):
        cid = e["id"]
        if cid in pend or cid in done:
            total["repeats"] += 1
            return
        r = {"ns": e["ns"], "last": e["ns"], "exp": e["exp"], "kind": e["kind"], "sr": e["sr"], "got": {}}
        pend[cid] = r
        heapq.heappush(deadlines, (e["ns"] + wait_ns, cid))
        for s, v in early.pop(cid, {}).items():
            r["got"][s] = v
            r["last"] = max(r["last"], v[1])
        st["content"] += 1
        if len(r["got"]) == len(r["exp"]):
            emit(cid, r, now)

    def on_signal(e, now):
        cid, s = e["id"], e["sig"]
        r = pend.get(cid)
        if r is None:
            if cid in done:
                total["late"] += 1                        # its content already went out without it
            elif s not in early.setdefault(cid, {}):
                early[cid][s] = (e["score"], e["ns"])     # came before its content: hold it
                early_q.append((now, cid))
            else:
                total["repeats"] += 1
            return
        if s in r["got"]:
            total["repeats"] += 1
            return
        r["got"][s] = (e["score"], e["ns"])
        r["last"] = max(r["last"], e["ns"])
        if len(r["got"]) == len(r["exp"]):
            emit(cid, r, now)

    async def reader(work):
        # work: (topic, partition, handler) pairs read in turn on one connection, the content topic first in
        # each round, so a post is usually in place before its scores are read.
        c = await connect(a.url)
        consumers = {(t, p): Consumer.Single(f"rev2-joiner-{t}-p{p}") for t, p, _ in work}
        nxt, committed = {}, {}
        while True:
            more = False
            for t, p, handle in work:
                k = (t, p)
                start = PollingStrategy.Offset(nxt[k]) if k in nxt else PollingStrategy.Next()
                # Store this partition's offset on the broker at most once a second, not on every poll.
                commit = time.monotonic() - committed.get(k, 0) >= a.commit_s
                ms = await c.poll_messages(STREAM, t, consumer=consumers[k], partition_id=p,
                                           count=a.batch, auto_commit=commit, polling_strategy=start)
                if ms:
                    if commit:
                        committed[k] = time.monotonic()
                    nxt[k] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        handle(load(m.payload()), now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def sweeper():
        # Content past its wait limit goes out without the missing signals; old bookkeeping is dropped.
        while True:
            await asyncio.sleep(0.05)
            now = time.time_ns()
            while deadlines and deadlines[0][0] <= now:
                _, cid = heapq.heappop(deadlines)
                if cid in pend:
                    emit(cid, pend[cid], now)
            while early_q and now - early_q[0][0] > 3 * wait_ns:
                _, cid = early_q.popleft()
                if early.pop(cid, None) is not None:
                    total["orphans"] += 1                 # signals whose content never came
            while done_q and now - done_q[0][0] > a.remember_s * 1e9:
                done.pop(done_q.popleft()[1], None)

    async def sender():
        # Enriched content goes out on its own connection, one batch per partition per tick, so no reader
        # waits on a send and the broker sees a few larger writes, not many tiny ones.
        w = await connect(a.url)
        while True:
            await asyncio.sleep(a.flush_ms / 1000)
            if out:
                batch = dict(out)
                out.clear()
                await asyncio.gather(*(w.send_messages(STREAM, ENRICHED_TOPIC, Partitioning.partition_id(pid), ms)
                                       for pid, ms in batch.items()))

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of joins came inside the budget; partitions interleave,
                # so one fresh join alone doesn't mean the replay is finished
                if st["replayed"] and st["max_age"] < budget_ns:
                    caught_up[0] = True
                print(f"[{name}] catching up: replayed {st['replayed']:,} earlier joins this second", flush=True)
                st.clear()
                continue
            r, wt = lat.flush(), wait.flush()
            ww = sorted(win_wait)[len(win_wait) // 2] if win_wait else 0.0
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            exp = total["expected"] or 1
            print(f"[{name}] {r['n']:>5,} joins/s  last-signal-to-joined p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  "
                  f"p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  join-rate {100 * total['joined'] / exp:5.1f}%  "
                  f"{a.compare_window_s:g}s-window join-rate {100 * total['win_joined'] / exp:5.1f}%  "
                  f"content to enriched p50 {wt['p50']:5.0f}ms  p99 {wt['p99']:5.0f}ms  "
                  f"window p50 {ww:5.0f}ms  without a signal {st['partial']:>3,}/s  late {total['late']:,}  "
                  f"repeats {total['repeats']:,}  waiting {len(pend):,}", flush=True)
            st.clear()

    print(f"[{name}] owns partition numbers {mine} of {parts} (joiner {a.joiner} of {a.joiners}) on "
          f"{PRELIM_TOPIC} and {', '.join(signal_topic(s) for s in SIGNALS)} on {a.readers} connection(s); "
          f"wait limit {a.max_wait_s:g} s",
          flush=True)
    topics = [(PRELIM_TOPIC, on_content)] + [(signal_topic(s), on_signal) for s in SIGNALS]
    groups = [mine[r::a.readers] for r in range(a.readers)]
    readers = [reader([(t, p, h) for p in g for t, h in topics]) for g in groups if g]
    await asyncio.gather(report(), sender(), sweeper(), *readers)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--joiner", type=int, default=0)
    p.add_argument("--joiners", type=int, default=4, help="joiners splitting the partition numbers")
    p.add_argument("--partitions", type=int, default=32, help="partitions for new lane topics")
    p.add_argument("--max-wait-s", type=float, default=5, help="content goes out without a missing signal after this")
    p.add_argument("--compare-window-s", type=float, default=5, help="tumbling window size, for comparison only")
    p.add_argument("--budget-ms", type=float, default=50, help="p99 budget, last signal written to joined")
    p.add_argument("--readers", type=int, default=1, help="connections reading this joiner's partition numbers")
    p.add_argument("--poll-ms", type=float, default=10, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--flush-ms", type=float, default=10, help="how often enriched content is sent")
    p.add_argument("--commit-s", type=float, default=1, help="how often each partition's offset is stored")
    p.add_argument("--remember-s", type=float, default=60, help="how long a written-out content ID is remembered")
    asyncio.run(main(p.parse_args()))
