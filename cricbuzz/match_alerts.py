"""Match alerts: the second reader of the commentary lane. Decides the live match alerts (a wicket, the batting side
passing every 50 runs) from the same lines the commentary nodes show, and takes a wicket alert back when the
desk overturns that wicket on review.

It reads every partition with its own connection and its own position, so it never slows the commentary nodes
and they never slow it. Each alert is first recorded on the alerts-sent topic and then handed to the push
gateway (simulated here: the kit counts it). On start, or a restart, the service reads that record, then
rebuilds every live match from the commentary log: an alert already recorded is never sent again, a missed one
still young enough (--late-s) is sent late, an older one is skipped. Recording first means a crash in the few
ms between the record and the push can miss one alert; it never sends one twice.

Prints a line a second: ball-to-alert latency (created at the desk -> alert decided here) against --budget-ms,
then the alerts: wickets, milestones, wickets taken back after review, and alerts sent late after a restart.
--audit reads the alerts-sent record, prints how many alerts it holds and how many were recorded twice, and exits.
"""
import argparse
import asyncio
import gc
import json
import time

from apache_iggy import Consumer, PollingStrategy, SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, SENT, STREAM, Window, connect, ensure_topic, unpack


async def read_sent(c, name: str) -> tuple[set, int, int]:
    # Everything this service has recorded as sent today, from the start of the record.
    sent, dup, n, nxt = set(), 0, 0, None
    me = Consumer.Single(f"{name}-record")
    while True:
        ms = await c.poll_messages(STREAM, SENT, consumer=me, partition_id=0, count=5000, auto_commit=False,
                                   polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else PollingStrategy.First())
        for msg in ms:
            v = json.loads(msg.payload())
            key = (v["m"], v["l"], v["a"])
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
        print(f"[{a.name}] waiting for commentary_desk.py to create the commentary lane…", flush=True)
        await asyncio.sleep(1)

    sent, n0, dup0 = await read_sent(admin, a.name)
    print(f"[{a.name}] alerts already sent today, from the record: {n0:,} (recorded twice {dup0})", flush=True)
    seqs, runs = {}, {}                                  # match -> last seq; match -> (innings, runs)
    queue = []                                           # alerts decided, waiting to be recorded and pushed
    lat, st = Window(), dict(wicket=0, milestone=0, retract=0, late=0, stale=0, replayed=0, max_age=0.0)
    started, head, caught = time.monotonic(), set(), [None, False]
    gc.freeze()

    def alert(kind, v, line, now):
        key = (v["m"], line, kind)
        if key in sent:
            return                                       # recorded before: never sent twice
        age_ms = (now - v["ns"]) / 1e6
        if not caught[1]:
            if age_ms > a.late_s * 1000:
                st["stale"] += 1                         # too old to be worth a push now
                return
            st["late"] += 1
        else:
            lat.add(age_ms)
        sent.add(key)
        st[kind] += 1
        queue.append(SendMessage(json.dumps({"m": v["m"], "l": line, "a": kind, "sc": v["sc"], "ns": time.time_ns()},
                                            separators=(",", ":")).encode()))

    def apply(ms, now):
        for msg in ms:
            v = unpack(msg.payload())
            m, s = v["m"], v["s"]
            if s <= seqs.get(m, 0):
                continue                                 # a repeat: already decided
            seqs[m] = s
            inn, r = v["sc"][0], v["sc"][1]
            prev = runs.get(m)
            runs[m] = (inn, r)
            if s == 1:
                continue
            if not caught[1]:
                st["replayed"] += 1
                st["max_age"] = max(st["max_age"], (now - v["ns"]) / 1e6)
            if v["k"] == "ball" and v["e"] == "OUT":
                alert("wicket", v, v["l"], now)
            elif v["k"] == "edit" and v["e"] == "NOT OUT" and (m, v["l"], "wicket") in sent:
                alert("retract", v, v["l"], now)         # the wicket alert is taken back
            if prev and prev[0] == inn and r // 50 > prev[1] // 50:
                alert("milestone", v, v["l"], now)

    async def record():
        # Record, then push: one batch per tick from its own connection, never from inside the read loop.
        c = await connect(a.url)
        due = time.monotonic()
        while True:
            if queue:
                batch = queue[:]
                del queue[:len(batch)]
                await c.send_messages(STREAM, SENT, 0, batch)
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

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
                    continue
            if pid not in head:
                head.add(pid)
                if len(head) == PARTITIONS:
                    caught[0] = time.monotonic()
                    print(f"[{a.name}] rebuilt from the log in {caught[0] - started:.2f}s: {len(seqs)} live matches; "
                          f"sent late {st['late']}, skipped as too old {st['stale']}", flush=True)
            await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught[1]:
                if caught[0] is not None and st["replayed"] and st["max_age"] < a.budget_ms:
                    caught[1] = True
                print(f"[{a.name}] catching up: {st['replayed']:,} lines replayed this second", flush=True)
                st.update(replayed=0, max_age=0.0)
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            lead = (f"{r['n']:>3} alerts/s  ball-to-alert p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)"
                    if r["n"] else "  0 alerts/s")
            print(f"[{a.name}] {lead}  wickets {st['wicket']}  milestones {st['milestone']}  taken back after review "
                  f"{st['retract']}  sent late {st['late']}", flush=True)
            st.update(wicket=0, milestone=0, retract=0)

    await asyncio.gather(record(), report(), *(read(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="alerts")
    p.add_argument("--budget-ms", type=float, default=50, help="p99 budget for ball-to-alert (before the push gateway)")
    p.add_argument("--late-s", type=float, default=30, help="after a restart, send a missed alert only if younger than this")
    p.add_argument("--tick-ms", type=float, default=5, help="record decided alerts once per this many ms")
    p.add_argument("--poll-ms", type=float, default=1)
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--audit", action="store_true", help="print the alerts-sent record's totals and exit")
    asyncio.run(main(p.parse_args()))
