"""What an agent would see: is the Postgres review context as current as the Iggy topic?

It reads the topic itself (direct partition reads, the ground truth) and the Postgres table every 50 ms, and prints a
line a second:
- change-to-index freshness: from the review service writing a change to Iggy to the row being readable in Postgres
  (a resolved thread: its row gone), p50/p99/p99.9, PASS/FAIL against --budget-ms;
- stale answers: --lookups random thread lookups a second, as an agent would make, and how many returned an older
  version of the thread (or a resolved one) than the topic held at that moment; next to it, the same lookups answered
  from a snapshot taken when the agent's session started (--session-s ago at most), as a long-running agent that read
  its context once would answer them;
- overdue: changes older than the budget that are still not in the index.

While the CocoIndex app is down (its heartbeat stops) and until it has caught up again, seconds print as `catching up`
with the changes held in Iggy, and are not counted as live latency. At the end it waits for the index to converge and
compares every thread: missing, stale, resolved-but-still-indexed, and extra rows must all be 0.
"""
import argparse
import asyncio
import gc
import json
import random
import time

import asyncpg
from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, EVENTS, PG_URL, STREAM, TABLE, Window, heartbeat_age


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    while not await client.get_topic(STREAM, a.topic):
        await asyncio.sleep(0.5)
    start_ns = time.time_ns()
    truth = {}          # thread -> (seq, deleted)
    keys = []           # every thread id seen, for lookups
    resolved_at = {}    # thread -> ns it was resolved
    pending = {}        # thread -> [(seq, ns, deleted, timed)] not yet visible in the index
    held = {"n": 0, "oldest": None}
    caught_up_topic = asyncio.Event()

    async def read_truth():
        reader, nxt = Consumer.Single("context-check"), 0
        end = (await client.get_topic(STREAM, a.topic)).partitions[0]
        end = end.current_offset + 1 if end.messages_count else 0
        if end == 0:
            caught_up_topic.set()
        while True:
            msgs = await client.poll_messages(STREAM, a.topic, consumer=reader, partition_id=0, count=2000,
                                              auto_commit=False, polling_strategy=PollingStrategy.Offset(nxt))
            if not msgs:
                await asyncio.sleep(0.005)
                continue
            offline = heartbeat_age() > 0.5
            for m in msgs:
                d = json.loads(m.payload())
                k = d.get("thread_id") or d["_deleted"]
                deleted = "_deleted" in d
                if k not in truth:
                    keys.append(k)
                truth[k] = (d["seq"], deleted)
                if deleted:
                    resolved_at[k] = d["ns"]
                timed = d["ns"] >= start_ns and not d.get("snap")
                if timed and offline:              # written while the indexer was down: held in Iggy, judged apart
                    timed = False
                    held["n"] += 1
                    held["oldest"] = held["oldest"] or d["ns"]
                pending.setdefault(k, []).append((d["seq"], d["ns"], deleted, timed))
            nxt = msgs[-1].offset() + 1
            if nxt >= end:
                caught_up_topic.set()

    async def open_table():
        conn = await asyncpg.connect(a.pg_url)
        while not await conn.fetchval("SELECT to_regclass($1)", a.table):   # CocoIndex creates it on start
            await asyncio.sleep(0.2)
        return conn

    async def read_index(conn):
        return dict(await conn.fetch(f'SELECT thread_id, seq FROM "{a.table}"'))

    asyncio.create_task(read_truth())
    await caught_up_topic.wait()
    tb = await open_table()
    gc.freeze()
    lat, idx = Window(), {}
    snapshot, session_t = dict(truth), time.monotonic()
    catching_up, offline_s = True, False
    passed = failed = 0
    t_end = time.monotonic() + a.duration
    next_report = time.monotonic() + 1
    stale_live = stale_snap = looked = 0
    while time.monotonic() < t_end:
        idx = await read_index(tb)
        now = time.time_ns()
        if heartbeat_age() > 0.5:
            offline_s = True
        for k in list(pending):
            entries = pending[k]
            if k in idx:
                done = [e for e in entries if not e[2] and e[0] <= idx[k]]
            elif any(e[2] for e in entries):        # threads never reopen: no row + a pending delete = applied
                done = entries
            else:
                done = []
            for e in done:
                if e[3]:
                    lat.add((now - e[1]) / 1e6)
            rest = [e for e in entries if e not in done]
            if rest:
                pending[k] = rest
            else:
                del pending[k]
        # Agent lookups: threads open now, or resolved within the session window.
        for _ in range(a.lookups // 20):
            k = random.choice(keys)
            if k in resolved_at and now - resolved_at[k] > a.session_s * 1e9:
                continue
            seq, deleted = truth[k]
            looked += 1
            if (k in idx) if deleted else (idx.get(k, 0) < seq):
                stale_live += 1
            s = snapshot.get(k)
            if s != (seq, deleted) and not (deleted and s is None):
                stale_snap += 1
        if time.monotonic() >= next_report:
            next_report += 1
            w = lat.flush()
            overdue = sum(1 for es in pending.values() for e in es if now - e[1] > a.budget_ms * 1e6)
            ok = w["p99"] <= a.budget_ms and overdue == 0
            sl = "stale answers %.1f%% (snapshot from %ds ago: %.1f%%)" % (
                100 * stale_live / max(1, looked), time.monotonic() - session_t, 100 * stale_snap / max(1, looked))
            # Caught up only after a whole second in which every change was inside the budget: one fresh change is
            # not enough, the replayed backlog lands in the same seconds.
            clean = w["n"] and w["max"] <= a.budget_ms and overdue == 0
            if offline_s or catching_up and not clean:
                catching_up = True
                oldest = (now - held["oldest"]) / 1e9 if held["oldest"] and pending else 0
                print(f"[check] catching up: {w['n']:>5,} changes indexed  {sum(map(len, pending.values())):,} not yet "
                      f"in the index, {held['n']:,} held in Iggy while the indexer was down, oldest {oldest:.1f} s  "
                      f"{'(indexer down)' if offline_s else ''}  {sl}", flush=True)
            elif w["n"] == 0:
                print(f"[check]     0 changes indexed  overdue {overdue}  {sl}", flush=True)
            else:
                if catching_up:
                    held["n"], held["oldest"] = 0, None
                catching_up = False
                passed, failed = passed + ok, failed + (not ok)
                print(f"[check] {w['n']:>5,} changes/s indexed  change-to-index p50 {w['p50']:5.0f}ms "
                      f"p99 {w['p99']:5.0f}ms p99.9 {w['p999']:5.0f}ms  {'PASS' if ok else 'FAIL'} "
                      f"(<{a.budget_ms:.0f}ms)  overdue {overdue}  {sl}", flush=True)
            offline_s = False
            stale_live = stale_snap = looked = 0
            if time.monotonic() - session_t >= a.session_s:
                snapshot, session_t = dict(truth), time.monotonic()
        await asyncio.sleep(0.05)
    print(f"[check] totals: live seconds PASS {passed} FAIL {failed}", flush=True)

    # Converge, then compare every thread.
    deadline = time.monotonic() + a.wait_s
    while True:
        await asyncio.sleep(1)
        idx = await read_index(tb)
        missing = sum(1 for k, (s, d) in truth.items() if not d and k not in idx)
        stale = sum(1 for k, (s, d) in truth.items() if not d and k in idx and idx[k] < s)
        zombie = sum(1 for k, (s, d) in truth.items() if d and k in idx)
        extra = sum(1 for k in idx if k not in truth)
        bad = missing + stale + zombie + extra
        if not bad or time.monotonic() > deadline:
            break
    open_n = sum(1 for s, d in truth.values() if not d)
    print(f"[verify] threads on the topic {len(truth):,} (open {open_n:,}, resolved {len(truth) - open_n:,})  rows in "
          f"Postgres {len(idx):,}  missing {missing}  stale {stale}  resolved but still indexed {zombie}  extra {extra}",
          flush=True)
    print("[verify] OK" if not bad else "[verify] MISMATCH", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--pg-url", default=PG_URL)
    p.add_argument("--topic", default=EVENTS)
    p.add_argument("--table", default=TABLE)
    p.add_argument("--budget-ms", type=float, default=1000, help="change-to-index freshness budget")
    p.add_argument("--lookups", type=int, default=400, help="agent thread lookups per second")
    p.add_argument("--session-s", type=float, default=300, help="agent session length (snapshot age at most)")
    p.add_argument("--duration", type=float, default=3600)
    p.add_argument("--wait-s", type=float, default=60, help="how long to wait for the index to converge at the end")
    asyncio.run(main(p.parse_args()))
