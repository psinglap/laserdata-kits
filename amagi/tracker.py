"""The quartile tracker: it reads the ad segments the stitchers serve and fires each quartile beacon once, when the
player fetches the segment that holds that quartile point.

Run several (ci.sh runs two) with --index 0, 1 and the same --trackers: each owns the lane partitions p where
p % trackers == index, so a session's segments always reach one tracker, in order. It reads those partitions
directly (its own offsets, no consumer group) on its own connection.

For each segment it works out the quartile points inside it (common.due_quartiles) and fires the ones this
impression has not fired yet. The (impression, quartile) pair is the beacon's ID: a segment event delivered twice
(a stitcher retry, an at-least-once redelivery, a replay after a restart) fires nothing new. fire() is where your
HTTP call to the ad network's tracking URL goes; in the kit it only counts.

Every beacon fired is also written to beacons-fired, keyed by session on the same partitions, from a separate task
in small batches, so the read loop never waits on a send. On start the tracker reads its partitions of
beacons-fired back from --lookback-s ago, then replays its lane partitions from the same moment: segments it
missed while it was down fire late but once, and segments already handled fire nothing. On SIGTERM (a rolling
deploy) it stops reading, writes out every beacon record it holds, and exits.

Prints a line a second: segment-to-tracker latency (served by the stitcher -> handled here), PASS or FAIL against
--budget-ms, beacons fired (and impressions, the start beacons), repeats dropped, and beacons that fired after the
viewer's ad had already ended.
"""
import argparse
import asyncio
import gc
import signal
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, FIRED, PARTITIONS, QUARTILES, SEGMENTS, STREAM, Stalls, Window, connect,
                    due_quartiles, ensure_topics, load, msg)


async def main(a):
    assert 0 <= a.index < a.trackers, "--index must be in 0..--trackers-1"
    me = a.name
    await ensure_topics(await connect(a.url), SEGMENTS, FIRED)
    reader, writer = await connect(a.url), await connect(a.url)     # one connection each way
    P = PARTITIONS[SEGMENTS]
    mine = [p for p in range(P) if p % a.trackers == a.index]
    start_ns = time.time_ns()
    since = PollingStrategy.Timestamp(start_ns // 1000 - int(a.lookback_s * 1e6))   # microseconds
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(s, stop.set)

    fired = {}                       # impression -> bitmask of the quartiles already fired
    born = deque()                   # (first seen ns, impression), to forget impressions after --keep-s
    pending = [defaultdict(list)]    # partition -> beacon records not yet written
    lat, st, run, stalls = Window(), defaultdict(int), defaultdict(int), Stalls()
    caught = set()

    # 1. What did this tracker (or the one before it) already fire? Read it back from beacons-fired.
    who = Consumer.Single(f"{me}-rebuild")
    for p in mine:
        strategy = since
        while True:
            ms = await reader.poll_messages(STREAM, FIRED, consumer=who, partition_id=p, count=a.batch,
                                            auto_commit=False, polling_strategy=strategy)
            for m in ms:
                e = load(m.payload())
                if e["i"] not in fired:
                    born.append((e["sg"], e["i"]))
                fired[e["i"]] = fired.get(e["i"], 0) | (1 << e["q"])
                run["rebuilt"] += 1
            if len(ms) < a.batch:
                break
            strategy = PollingStrategy.Offset(ms[-1].offset() + 1)
    print(f"[{me}] rebuilt in {(time.time_ns() - start_ns) / 1e9:.2f}s: {run['rebuilt']:,} beacons already fired in "
          f"the last {a.lookback_s:g} s on partitions {mine}; replaying the lane from the same moment", flush=True)
    gc.freeze()                      # keep start-up tables out of the collector's way

    def fire(e, q, now, p):
        # Your HTTP call to the ad network's tracking URL for this quartile goes here.
        pending[0][p].append(SendMessage(msg(s=e["s"], i=e["i"], q=q, ad=e["ad"], sg=e["ns"], tr=me, ns=now)))
        st["beacons"] += 1
        if q == 0:
            st["imps"] += 1
        if now > e["a0"] + e["d"] * 1_000_000:
            run["late"] += 1         # the viewer's ad had already ended on screen

    def handle(e, now, p):
        replay = e["ns"] < start_ns   # published before this tracker started: catching up, not live latency
        if replay:
            run["replayed"] += 1
        else:
            lat.add((now - e["ns"]) / 1e6)
            st["segments"] += 1
        qs = due_quartiles(e["g"], e["n"], e["d"], e["sm"])
        if not qs:
            return
        i = e["i"]
        mask = fired.get(i)
        if mask is None:
            mask = 0
            born.append((e["ns"], i))
        for q in qs:
            if mask >> q & 1:
                run["repeats"] += 1  # this beacon was already fired: count it once
                continue
            mask |= 1 << q
            fire(e, q, now, p)
            if replay:
                run["replay_fired"] += 1
        fired[i] = mask

    async def read():
        who = {p: Consumer.Single(f"{me}-{p}") for p in mine}
        nxt = {}
        while not stop.is_set():
            more = False
            for p in mine:
                ms = await reader.poll_messages(STREAM, SEGMENTS, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        handle(load(m.payload()), now, p)
                if len(ms) == a.batch:
                    more = True      # a replay or a burst: read on at once
                elif p not in caught:
                    caught.add(p)
                    if len(caught) == len(mine):
                        print(f"[{me}] caught up in {(time.time_ns() - start_ns) / 1e9:.2f}s: {run['replayed']:,} "
                              f"segments replayed from the log, {run['replay_fired']:,} beacons fired late but once",
                              flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send():
        # Beacon records go out from here, one batch per partition every --flush-ms, so the read loop never waits.
        due = time.monotonic()
        while True:
            batch, pending[0] = pending[0], defaultdict(list)
            if batch:
                await asyncio.gather(*(writer.send_messages(STREAM, FIRED, Partitioning.partition_id(p), ms)
                                       for p, ms in batch.items()))
            elif stop.is_set() and reading.done():
                return
            due += a.flush_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            if len(caught) < len(mine):
                print(f"[{me}] catching up: {run['replayed']:,} segments replayed so far", flush=True)
                st.clear()
                continue
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{me}] {st['segments']:>6,} segments/s  segment-to-tracker p50 {r['p50']:5.1f}ms  "
                  f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"beacons fired {st['beacons']:,} (impressions {st['imps']:,})  repeats dropped {run['repeats']:,}  "
                  f"fired after the ad ended {run['late']:,}  {stalls.read()}", flush=True)
            st.clear()
            cut = time.time_ns() - int(a.keep_s * 1e9)
            while born and born[0][0] < cut:         # forget impressions that ended long ago
                fired.pop(born.popleft()[1], None)

    reading = asyncio.ensure_future(read())
    sending = asyncio.ensure_future(send())
    side = [asyncio.ensure_future(c) for c in (report(), stalls.watch())]
    await reading
    await sending                     # returns once every pending beacon record is written
    for t in side:
        t.cancel()
    print(f"[{me}] stopped: every beacon record written; {sum(bin(m).count('1') for m in fired.values()):,} beacons "
          f"held ({', '.join(QUARTILES)})", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="tracker-1")
    p.add_argument("--index", type=int, default=0, help="this tracker's index, 0..--trackers-1")
    p.add_argument("--trackers", type=int, default=1, help="trackers sharing the lane; same on every tracker")
    p.add_argument("--budget-ms", type=float, default=50, help="segment-to-tracker p99 budget")
    p.add_argument("--lookback-s", type=float, default=30,
                   help="on start, rebuild fired beacons and replay segments from this far back (longer than a restart)")
    p.add_argument("--keep-s", type=float, default=600, help="forget an impression's beacons after this long")
    p.add_argument("--flush-ms", type=float, default=5, help="how often beacon records are written")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=2000)
    asyncio.run(main(p.parse_args()))
