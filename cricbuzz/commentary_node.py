"""A commentary node: reads every commentary line and correction from the lane and holds every live match's
ball-by-ball feed, the way a node of the commentary service answers a match page.

It reads every partition with its own connection and its own position (no shared consumer group), so adding
nodes before a big match rebalances nothing. It keeps, per live match, the recent lines by line id with their
version, and the resulting score. A correction replaces the line it names, and only if its version is newer, so
a fan never sees a corrected line flip back.

On start it rebuilds every live match from the log (every line and correction since the opening lines, kept for
a match day), then times live lines. Prints a line a second:
  * line-to-node latency (created at the desk -> on this node), PASS or FAIL against --budget-ms;
  * corrections live on this node that second (wickets overturned, boundaries re-scored) and the slowest one:
    how long a fan reading that match still saw the old line after the desk corrected it;
  * lines held at an older version than the desk's newest (must stay 0) and sequence gaps (must stay 0).
"""
import argparse
import asyncio
import gc
import time

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, Window, connect, match_ids, unpack

KEEP = 300                                               # recent lines held per match


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, LANE):
        print(f"[{a.name}] waiting for commentary_desk.py to create the commentary lane…", flush=True)
        await asyncio.sleep(1)

    matches = match_ids(a.matches)
    feed, seqs, score = {}, {}, {}                       # match -> {line: (ver, over, event, text)}; last seq; score
    gc.freeze()                                          # keep startup tables out of the collector's way

    lat, st = Window(), dict(live=0, fixes=0, overturned=0, slow_fix=0.0, old=0, gaps=0, replayed=0, rebuilt=0,
                             max_age=0.0)
    started, head, caught = time.monotonic(), set(), [None, False]   # partitions at the head; [rebuilt at, timing live]

    def apply(ms, now):
        for msg in ms:
            v = unpack(msg.payload())
            m, s = v["m"], v["s"]
            last = seqs.get(m)
            if last is not None:
                if s <= last:
                    continue                             # a repeat (at-least-once redelivery): skip it
                if s > last + 1:
                    st["gaps"] += 1                      # a line for this match never arrived
            seqs[m] = s
            score[m] = v["sc"]
            lines = feed.setdefault(m, {})
            held = lines.get(v["l"])
            if v["k"] == "edit" and held is not None and held[0] >= v["v"]:
                st["old"] += 1                           # would put an older version back: must never happen
                continue
            if v["k"] != "over":
                lines[v["l"]] = (v["v"], v["o"], v["e"], v["t"])
                if len(lines) > KEEP:
                    del lines[min(lines)]
            if s == 1:
                continue                                 # the opening line is not a live line
            age = now - v["ns"]
            if caught[1]:
                lat.add(age / 1e6)
                st["live"] += 1
                if v["k"] == "edit":
                    st["fixes"] += 1
                    st["overturned"] += v["e"] == "NOT OUT"
                    st["slow_fix"] = max(st["slow_fix"], age / 1e6)
            else:
                st["replayed"] += 1
                st["rebuilt"] += 1
                st["max_age"] = max(st["max_age"], age / 1e6)

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        while True:
            ms = await c.poll_messages(STREAM, LANE, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else PollingStrategy.First())
            if ms:
                apply(ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                             # more is waiting (the rebuild): read on at once
            if pid not in head:
                head.add(pid)                            # this partition's replay reached the head of the log
                if len(head) == PARTITIONS:
                    caught[0] = time.monotonic()
                    live = [m for m in matches if m in feed]
                    print(f"[{a.name}] rebuilt from the log in {caught[0] - started:.2f}s: commentary for {len(live)} of "
                          f"{len(matches)} live matches, {st['rebuilt']:,} lines and corrections replayed", flush=True)
            await asyncio.sleep(a.poll_ms / 1000)        # the desk writes once per --tick-ms; wait for the next write

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught[1]:
                # Live once the log is replayed and a whole second of lines arrived inside the budget;
                # partitions interleave, so one fresh line alone doesn't mean the replay is finished.
                if caught[0] is not None and st["replayed"] and st["max_age"] < a.budget_ms:
                    caught[1] = True
                print(f"[{a.name}] catching up: {st['replayed']:,} lines replayed this second, "
                      f"{len(head)} of {PARTITIONS} partitions at the head of the log", flush=True)
                st.update(replayed=0, max_age=0.0)
                lat.flush()
                continue
            r, n = lat.flush(), st["live"]
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            top = matches[0]
            sc = score.get(top, [0, 0, 0, 0])
            print(f"[{a.name}] {n:>5,} lines/s  line-to-node p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  corrections live {st['fixes']} "
                  f"(overturned {st['overturned']}), slowest +{st['slow_fix']:.1f}ms  old lines held {st['old']}  "
                  f"gaps {st['gaps']}  {top} {sc[1]}/{sc[2]}", flush=True)
            st.update(live=0, fixes=0, overturned=0, slow_fix=0.0)

    await asyncio.gather(report(), *(read(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="commentary-1")
    p.add_argument("--matches", type=int, default=30, help="same as commentary_desk.py")
    p.add_argument("--budget-ms", type=float, default=20, help="p99 budget for line-to-node")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
