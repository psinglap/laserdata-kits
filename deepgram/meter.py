"""The usage meter, fed by the usage lane: live streaming usage into per-project, per-minute usage summaries.

Reads the usage topic directly, partition by partition, on --readers connections of its own
(no consumer group: a group reader visits one partition per poll, so with 16 partitions a record waited for its
partition's turn). Each message is one live stream's billable
audio so far (the resulting total, with a per-stream sequence), so the meter adds only what is new:
  * a message whose sequence it has already applied (a retry, or a replay after a restart) adds nothing;
  * a newer one adds its total minus the last total applied for that stream, so a lost message in between
    is covered by the next one;
  * a stream's last message (fin=1) is reconciled: the audio added to the summaries for that stream must
    equal the engine's own final total for it, to the millisecond (a stream the meter joined mid-way, or a
    broken rule, shows up as mismatched).
Apache Iggy delivers at least once; counting once comes from this rule, which is why the lane carries totals.

Usage is summed by minute, project, model and API key (the dimensions of the usage breakdown), and every
--summary-ms the changed per-project totals for the current minute are published to usage-summaries, as
resulting values, for the Console's usage view and the billing platform sync.

Prints usage-to-summary p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business
numbers: audio metered this minute, streams reconciled, retries absorbed, and what a meter that adds every
message's chunk would have over-billed.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import DEFAULT_URL, PARTITIONS, STREAM, SUMMARIES, USAGE, Window, ensure_topics, load, msg, usd


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), USAGE, SUMMARIES)
    out = await (await own_client()).producer(STREAM, SUMMARIES, topic_partitions_count=PARTITIONS[SUMMARIES])
    lat, stats = Window(), defaultdict(float)
    last = {}                               # stream -> (sequence, audio ms) last applied
    closed = {}                             # stream -> when it closed; later copies of its messages add nothing
    applied = defaultdict(int)              # stream -> audio ms added to the summaries for it
    metered = defaultdict(int)              # (minute, project, model, API key) -> audio ms
    by_project = defaultdict(int)           # (minute, project) -> audio ms
    project_usd = defaultdict(float)        # (minute, project) -> USD at list price
    changed = set()                         # (minute, project) changed since the last summary
    sseq = defaultdict(int)                 # (minute, project) -> summary sequence
    tot = defaultdict(float)                # since start: reconciled, mismatched, absorbed, naive_ms, metered_ms
    caught_up = [False]                     # a new reader first replays usage from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_usage(m):
        u = load(m.payload())
        now = time.time_ns()
        age = (now - u["ns"]) / 1e6
        sid = u["id"]
        tot["naive_ms"] += u["d"]           # what a meter that adds every message's chunk would count
        seq, ms = last.get(sid, (0, 0))
        if sid in closed or u["seq"] <= seq:
            tot["absorbed"] += 1            # a retry or a replay: already counted
            return
        if not caught_up[0]:                # still replaying: track the oldest new message seen this second
            stats["replayed"] += 1          # (a retry carries its original send time, so it is left out)
            stats["max_age"] = max(stats["max_age"], age)
        add = u["ms"] - ms
        last[sid] = (u["seq"], u["ms"])
        minute = u["ns"] // 60_000_000_000
        metered[(minute, u["pj"], u["m"], u["k"])] += add
        by_project[(minute, u["pj"])] += add
        project_usd[(minute, u["pj"])] += usd(add, u["m"])
        applied[sid] += add
        changed.add((minute, u["pj"]))
        tot["metered_ms"] += add
        stats["applied"] += 1
        if caught_up[0]:
            lat.add(age)                    # live usage, from the engine's send to the summary updated
        if u["fin"]:
            # The engine's own total for the stream against what the meter applied for it.
            tot["reconciled" if applied.pop(sid) == u["ms"] else "mismatched"] += 1
            closed[sid] = now
            del last[sid]

    async def send_summaries():
        async with out:
            while True:
                await asyncio.sleep(a.summary_ms / 1000)
                if not changed:
                    continue
                batch = []
                for k in list(changed):
                    sseq[k] += 1
                    batch.append(SendMessage(msg(min=k[0], pj=k[1], seq=sseq[k], ms=by_project[k],
                                                 usd=round(project_usd[k], 6))))
                changed.clear()
                await out.send(batch)
                stats["summaries"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, USAGE, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_usage(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of usage arrived inside the budget; partitions interleave,
                # so one fresh message alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier usage messages this second",
                      flush=True)
                stats.clear()
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} usage msgs/s  usage-to-summary p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"summaries sent {int(stats['summaries']):,}", flush=True)
            minute = time.time_ns() // 60_000_000_000
            ms_now = sum(v for (mm, _), v in by_project.items() if mm == minute)
            usd_now = sum(v for (mm, _), v in project_usd.items() if mm == minute)
            over = tot["naive_ms"] - tot["metered_ms"]
            print(f"[usage] minute {minute % 10000}: audio metered {ms_now / 3_600_000:,.2f} h (${usd_now:,.2f} at list)  "
                  f"projects {sum(1 for (mm, _) in by_project if mm == minute):,}  "
                  f"streams reconciled {int(tot['reconciled']):,}/{int(tot['reconciled'] + tot['mismatched']):,}  "
                  f"retries absorbed {int(tot['absorbed']):,}  "
                  f"without dedupe +{over / 1000:,.0f} s (+${usd(over, 'nova-3'):,.4f}, "
                  f"{100 * over / tot['metered_ms'] if tot['metered_ms'] else 0:.2f}%)", flush=True)
            stats.clear()
            old = minute - 2                # forget minutes that ended more than a minute ago
            for d in (metered, by_project, project_usd, sseq):
                for k in [k for k in d if k[0] <= old]:
                    del d[k]
            gone = time.time_ns() - 300_000_000_000      # closed-stream markers kept five minutes
            for k in [k for k, t in closed.items() if t < gone]:
                del closed[k]

    parts = list(range((await (await own_client()).get_topic(STREAM, USAGE)).partitions_count))
    await asyncio.gather(send_summaries(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the usage partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="meter")
    p.add_argument("--budget-ms", type=int, default=50, help="usage-to-summary p99 budget for the lane")
    p.add_argument("--summary-ms", type=int, default=250, help="publish changed per-project totals this often")
    asyncio.run(main(p.parse_args()))
