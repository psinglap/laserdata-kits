"""A live node: the service that feeds open Live views (Activity > Live and the Web Analytics Live tab), fed by
the live events lane.

It keeps the set of projects that have an open view on this node. A project's events all sit on one lane
partition, so the node reads only the partitions that hold a watched project, directly, on --readers
connections of its own, and nothing else. When a viewer opens a project on a partition it is not reading
yet, it starts that partition at the time of the open; when the last viewer on a partition leaves, it stops
reading it. No consumer group, no offsets stored on the broker, no other node involved.

For every watched project it:
  * shows each event (here: counts it and times it from capture to this node),
  * checks the project's sequence numbers, so an event that never arrived is counted as missed, not guessed,
  * keeps the people seen in the last --online-s seconds (the "users online" number),
  * fills a newly opened view with the project's last --replay-s seconds from the retained lane, read on
    a separate connection, so the view does not open empty.

Every --churn-every seconds one viewer leaves and another project is opened (big projects are opened more
often than small ones), as people come and go during the day.

Prints capture-to-live p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business
side: events shown, events missed, users online, and how much of the firehose this node had to read.
"""
import argparse
import asyncio
import gc
import itertools
import os
import random
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, LANE, LAUNCH, STREAM, Stalls, Window, ensure_topics, load, part, token, watched


async def main(a):
    async def own_client():                 # one connection per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    parts = await ensure_topics(await own_client(), LANE, partitions={LANE: a.partitions})
    r = random.Random(5)
    cum = list(itertools.accumulate(1 / (i + 1) ** a.skew for i in range(a.projects)))

    watching = {}                           # token -> time the view opened (ns)
    on_part = defaultdict(set)              # partition -> watched tokens on it
    start_us = {}                           # partition -> read it from this time (µs) until the first message
    pos = {}                                # partition -> next offset to read
    last_seq = {}                           # (token, writer) -> newest sequence shown
    online = defaultdict(dict)              # token -> {distinct_id: last seen ns}
    replays = asyncio.Queue()
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    totals = defaultdict(int)
    caught_up = [False]

    def open_view(tok):
        if tok in watching:
            return
        now = time.time_ns()
        watching[tok] = now
        p = part(tok, parts)
        if not on_part[p]:
            start_us[p] = now // 1000       # a partition nobody here read: start at the time of the open
            pos.pop(p, None)
        on_part[p].add(tok)
        replays.put_nowait((tok, p, now))
        stats["opened"] += 1

    def close_view(tok):
        watching.pop(tok, None)
        for key in [k for k in last_seq if k[0] == tok]:
            del last_seq[key]
        online.pop(tok, None)
        p = part(tok, parts)
        on_part[p].discard(tok)
        if not on_part[p]:                  # nobody here watches this partition any more: stop reading it
            del on_part[p]
            pos.pop(p, None)
            start_us.pop(p, None)
        stats["closed"] += 1

    for i in watched(a.projects, a.viewers):
        open_view(token(i))
    open_view(LAUNCH)                       # the team shipping a launch keeps Live open all day
    stats["opened"] = 0
    replays = asyncio.Queue()               # the start-up views open with the node, not as new viewers
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_message(m):
        d = load(m.payload())
        tok = d["t"]
        if tok not in watching or d["ns"] < watching[tok]:
            stats["other"] += 1             # another project on the same partition: read, not shown
            return
        age = (time.time_ns() - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        key = (tok, d.get("b", 0))          # sequence numbers are per project per writer (bridge)
        s, last = d["s"], last_seq.get(key)
        if last is not None:
            if s <= last:
                stats["dup"] += 1           # already shown
                return
            if s > last + 1:
                stats["missed"] += s - last - 1
        last_seq[key] = s
        online[tok][d["d"]] = d["ns"]
        stats["shown"] += 1
        if tok == LAUNCH:
            stats["launch"] += 1

    async def read(r_i):
        # The partitions this connection owns, each from the open time, then from our own position.
        c = await own_client()
        who = {}
        while True:
            more = False
            for p in [p for p in list(on_part) if p % a.readers == r_i]:
                if p not in on_part:
                    continue                # its last viewer left while this round was polling
                if p not in who:
                    who[p] = Consumer.Single(f"{a.name}-{os.getpid()}-p{p}")
                strategy = (PollingStrategy.Offset(pos[p]) if p in pos
                            else PollingStrategy.Timestamp(start_us[p]))      # a new one: from the open
                ms = await c.poll_messages(STREAM, LANE, consumer=who[p], partition_id=p,
                                           count=a.batch, auto_commit=False, polling_strategy=strategy)
                if ms and p in on_part:
                    pos[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_message(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def replayer():
        # Fill each newly opened view with that project's last --replay-s seconds, on a connection of its own,
        # one view at a time, so the live readers never wait for it.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}-replay")
        while True:
            tok, p, opened_ns = await replays.get()
            strategy = PollingStrategy.Timestamp((opened_ns - int(a.replay_s * 1e9)) // 1000)
            n, oldest, done = 0, 0.0, False
            while not done:
                ms = await c.poll_messages(STREAM, LANE, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False, polling_strategy=strategy)
                if not ms:
                    break
                for m in ms:
                    d = load(m.payload())
                    if d["ns"] >= opened_ns:
                        done = True         # from here on the live reader shows it
                        break
                    if d["t"] == tok:
                        n += 1
                        oldest = max(oldest, (opened_ns - d["ns"]) / 1e9)
                strategy = PollingStrategy.Offset(ms[-1].offset() + 1)
                await asyncio.sleep(0)
            stats["filled"] += n
            stats["fill_age"] = max(stats["fill_age"], oldest)
            stats["fills"] += 1

    async def churn():
        due = time.monotonic()
        while True:
            due += a.churn_every
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            if not caught_up[0]:
                continue
            leaving = [t for t in watching if t != LAUNCH]
            if leaving:
                close_view(r.choice(leaving))
            while True:                     # big projects get opened more often than small ones
                tok = token(r.choices(range(a.projects), cum_weights=cum)[0])
                if tok not in watching:
                    break
            open_view(tok)

    async def report_line():
        while True:
            await asyncio.sleep(1)
            now = time.time_ns()
            cut = now - int(a.online_s * 1e9)
            for people in online.values():  # users online: people seen in the last --online-s seconds
                for k in [k for k, ns in people.items() if ns < cut]:
                    del people[k]
            if not caught_up[0]:
                # caught up once a whole second of shown events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the node is current
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: {int(stats['replayed']):,} events this second, oldest "
                      f"{stats['max_age']:.0f}ms, views open {len(watching):,}", flush=True)
                stats.clear()
                lat.flush()
                continue
            res = lat.flush()
            ok = "idle" if not res["n"] else "PASS" if res["p99"] < a.budget_ms else "FAIL"
            for k in ("shown", "other", "missed", "dup", "opened", "fills", "filled", "launch"):
                totals[k] += int(stats[k])
            read = stats["shown"] + stats["other"]
            print(f"[{a.name}] {res['n']:>7,} msgs/s  capture-to-live p50 {res['p50']:6.1f}ms  p99 {res['p99']:6.1f}ms  "
                  f"p99.9 {res['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  views {len(watching):,}  partitions read "
                  f"{len(on_part)}/{parts}  read {int(read):,}/s (other projects {int(stats['other']):,})  "
                  f"missed {int(stats['missed'])} (total {totals['missed']})  dup {int(stats['dup'])}  users online "
                  f"{sum(len(v) for v in online.values()):,} (launch {len(online.get(LAUNCH, ())):,}, "
                  f"{int(stats['launch']):,} ev/s)  opened {int(stats['opened'])} filled {int(stats['fills'])} "
                  f"with {int(stats['filled']):,} events (oldest {stats['fill_age']:.1f}s)  {stalls.read()}",
                  flush=True)
            stats.clear()

    await asyncio.gather(*(read(i) for i in range(a.readers)), replayer(), churn(), report_line(), stalls.watch())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="live")
    p.add_argument("--projects", type=int, default=100000, help="projects capture sends for (as in capture.py)")
    p.add_argument("--skew", type=float, default=1.0, help="as in capture.py: who gets opened more often")
    p.add_argument("--viewers", type=int, default=60, help="projects with an open Live view on this node at start")
    p.add_argument("--partitions", type=int, default=256, help="lane partitions, used if this creates the topic")
    p.add_argument("--budget-ms", type=int, default=100, help="capture-to-live p99 budget")
    p.add_argument("--online-s", type=float, default=60, help="window for users online")
    p.add_argument("--replay-s", type=float, default=60, help="a newly opened view is filled with this many seconds")
    p.add_argument("--churn-every", type=float, default=2.0, help="seconds between one viewer leaving and one opening")
    p.add_argument("--readers", type=int, default=2, help="connections reading the watched partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=2000)
    asyncio.run(main(p.parse_args()))
