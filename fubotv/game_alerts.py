"""Game Alerts for key plays: reads the game-moments lane and decides each key-play push the moment its clip is
playable, so a tap on the alert always opens the play.

  * key play flagged:          held, waiting for its clip;
  * clip ready:                the alert is decided and handed to push (unless the call was overturned);
  * review overturned first:   the alert is never pushed ("held back by review");
  * review overturned later:   a correction follows the alert that already went out.

It reads every partition with its own position, on its own connection, so it slows nothing else that reads the
lane. Each alert is first recorded on the alerts-sent topic and then handed to the push gateway (simulated here:
the kit counts it). On start, or a restart, it reads that record, then rebuilds every live game from the lane: an
alert already recorded is never pushed again, a missed one still young enough (--late-s) is pushed late, an older
one is skipped. Recording first means a crash in the few ms between the record and the push can miss one alert;
it never pushes one twice.

Prints a line a second: clip-to-alert latency (clip ready on the lane -> alert decided here) against --budget-ms,
then the alerts pushed, the key plays waiting for their clip, alerts held back or corrected after a review, gaps,
and the median play-to-push time (dominated by the time to cut the clip).
--audit reads the alerts-sent record, prints how many alerts it holds and how many were recorded twice, and exits.
"""
import argparse
import asyncio
import gc
import json
import statistics
import time

from apache_iggy import Consumer, PollingStrategy, SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, SENT, STREAM, Window, connect, ensure_topic, unpack


async def read_sent(c, name: str) -> tuple[set, int, int]:
    # Everything this service has recorded as pushed today, from the start of the record.
    sent, dup, n, nxt = set(), 0, 0, None
    me = Consumer.Single(f"{name}-record")
    while True:
        ms = await c.poll_messages(STREAM, SENT, consumer=me, partition_id=0, count=5000, auto_commit=False,
                                   polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else PollingStrategy.First())
        for msg in ms:
            v = json.loads(msg.payload())
            key = (v["g"], v["p"], v["a"])
            dup += key in sent
            sent.add(key)
            n += 1
        if len(ms) < 5000:
            return sent, n, dup
        nxt = ms[-1].offset() + 1


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, SENT, 1)
    if a.audit:
        sent, n, dup = await read_sent(admin, a.name + "-audit")
        print(f"[audit] alerts-sent record: {n:,} alerts, {len(sent):,} distinct, recorded twice {dup}", flush=True)
        return
    while not await admin.get_topic(STREAM, LANE):
        print(f"[{a.name}] waiting for moments.py to create the game-moments lane…", flush=True)
        await asyncio.sleep(1)

    sent, n0, dup0 = await read_sent(admin, a.name)
    print(f"[{a.name}] alerts already pushed today, from the record: {n0:,} (recorded twice {dup0})", flush=True)
    seqs = {}                                            # game -> last seq applied
    plays = {}                                           # (game, play) -> {"t","e","sc","ns","clip","over"}
    queue = []                                           # alerts decided, waiting to be recorded and pushed
    lat, p2p = Window(), []
    st = dict(pushed=0, held=0, corrected=0, upheld=0, late=0, stale=0, gaps=0, replayed=0, max_age=0.0)
    started, head, caught = time.monotonic(), set(), [None, False]
    gc.freeze()

    def alert(kind, g, p, info, v, now):
        key = (g, p, kind)
        if key in sent:
            return False                                 # recorded before: never pushed twice
        age_ms = (now - v["ns"]) / 1e6
        if not caught[1]:
            if age_ms > a.late_s * 1000:
                st["stale"] += 1                         # too old to be worth a push now
                return False
            st["late"] += 1                              # decided while this service was down: judged apart
        else:
            lat.add(age_ms)
            if kind == "play":
                p2p.append((now - info["ns"]) / 1e9)
        sent.add(key)
        queue.append(SendMessage(json.dumps({"g": g, "p": p, "a": kind, "t": info["t"], "e": info["e"],
                                             "sc": v["sc"], "ns": time.time_ns()}, separators=(",", ":")).encode()))
        return True

    def apply(ms, now):
        for msg in ms:
            v = unpack(msg.payload())
            g, s = v["g"], v["s"]
            last = seqs.get(g, 0)
            if s <= last:
                continue                                 # a repeat: already applied
            if last and s != last + 1:
                st["gaps"] += 1
            seqs[g] = s
            if not caught[1] and s > 1:
                st["replayed"] += 1
                st["max_age"] = max(st["max_age"], (now - v["ns"]) / 1e6)
            k, p = v["k"], v["p"]
            if k == "key":
                plays[(g, p)] = {"t": v["t"], "e": v["e"], "sc": v["sc"], "ns": v["ns"], "clip": False, "over": False}
            elif k == "clip" and (g, p) in plays:
                info = plays[(g, p)]
                info["clip"] = True
                if info["over"]:
                    st["held"] += 1                      # overturned before the clip was ready: never pushed
                elif alert("play", g, p, info, v, now):
                    st["pushed"] += 1
            elif k == "review" and (g, p) in plays:
                info = plays[(g, p)]
                if v["v"] == "upheld":
                    st["upheld"] += 1
                elif not info["over"]:
                    info["over"] = True
                    if (g, p, "play") in sent and alert("correction", g, p, info, v, now):
                        st["corrected"] += 1             # the alert went out first: a correction follows it
        if len(plays) > 20000:
            for key in list(plays)[:5000]:
                del plays[key]

    async def record():
        # Record, then push: one batch per tick, never from inside the read loop. It reuses the start-up
        # connection, which is otherwise idle: every new connection is a login on the broker.
        due = time.monotonic()
        while True:
            if queue:
                batch = queue[:]
                del queue[:len(batch)]
                await admin.send_messages(STREAM, SENT, 0, batch)
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def read():
        # One connection reads every partition in turn, each from this service's own position (direct partition
        # reads, no consumer group): alerts have a 50 ms budget, so one reader polling every few ms is enough.
        c = await connect(a.url)
        me = {p: Consumer.Single(f"{a.name}-{p}") for p in range(PARTITIONS)}
        nxt = {}
        while True:
            more = False
            for pid in range(PARTITIONS):
                ms = await c.poll_messages(STREAM, LANE, consumer=me[pid], partition_id=pid, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[pid]) if pid in nxt
                                           else PollingStrategy.First())
                if ms:
                    apply(ms, time.time_ns())
                    nxt[pid] = ms[-1].offset() + 1
                    more = more or len(ms) == a.batch
                if (not ms or len(ms) < a.batch) and pid not in head:
                    head.add(pid)                        # this partition's replay reached the head of the log
                    if len(head) == PARTITIONS:
                        caught[0] = time.monotonic()
                        print(f"[{a.name}] rebuilt from the log in {caught[0] - started:.2f}s: {len(seqs)} live "
                              f"games, {len(plays)} key plays; pushed late {st['late']}, skipped as too old "
                              f"{st['stale']}", flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught[1]:
                # Caught up only after a whole second in which every replayed entry was inside the budget.
                if caught[0] is not None and st["replayed"] and st["max_age"] < a.budget_ms:
                    caught[1] = True
                print(f"[{a.name}] catching up: {st['replayed']:,} moments replayed this second", flush=True)
                st.update(replayed=0, max_age=0.0, pushed=0, held=0, corrected=0, upheld=0)
                lat.flush()
                continue
            r = lat.flush()
            waiting = sum(1 for i in plays.values() if not i["clip"] and not i["over"])
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            lead = (f"{r['n']:>3} alerts/s  clip-to-alert p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  {ok} "
                    f"(<{a.budget_ms:g}ms)" if r["n"] else "  0 alerts/s")
            pp = f"{statistics.median(p2p):.1f}s" if p2p else "-"
            print(f"[{a.name}] {lead}  pushed {st['pushed']}  waiting for clip {waiting}  held back by review "
                  f"{st['held']}  corrected after push {st['corrected']}  pushed late {st['late']}  gaps {st['gaps']}  "
                  f"play-to-push {pp}", flush=True)
            p2p.clear()
            st.update(pushed=0, held=0, corrected=0, upheld=0)

    await asyncio.gather(record(), report(), read())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="alerts")
    p.add_argument("--budget-ms", type=float, default=50, help="p99 budget for clip-to-alert (before the push gateway)")
    p.add_argument("--late-s", type=float, default=60, help="after a restart, push a missed alert only if younger than this")
    p.add_argument("--tick-ms", type=float, default=5, help="record decided alerts once per this many ms")
    p.add_argument("--poll-ms", type=float, default=5,
                   help="wait between reads of a partition when nothing more is waiting; 5 ms is a tenth of the "
                        "alert budget and leaves the CPU to the rest of the box")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--audit", action="store_true", help="print the alerts-sent record's totals and exit")
    asyncio.run(main(p.parse_args()))
