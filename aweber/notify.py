"""A reader of the notifications lane: slackbot or email, the two consumers Imbi's ADR 0019 names for it.

Reads every partition of the `deployment` and `mention` topics directly on its own connection, tracking its own
position (no consumer group: a group reader visits one partition per poll, so a notification can wait for its
partition's turn; it also can't be left holding a partition by a member that is gone, the failure in
apache/iggy#4273). Each notification carries its key's sequence number, so a re-delivered one is recognised and
posted once, and a gap would show as missing.

  slackbot posts every deployment and mention; email sends production deployments and mentions.
  ("Posting" here is a counter; your Slack and SMTP calls go in post().)

Once a second the reader publishes its position (next offset per partition) to notification_readers/positions,
from its own task, for watchdog.py to compare with the topics' current offsets. A restarted reader starts from
the last position it published (a new reader from the current end of each topic): it reads the log before that
position only to rebuild its sequence table, posting nothing, so a re-sent copy is still recognised. What it posted
in its last second before a crash can be posted again; make post() idempotent on the notification id if that
matters.

--stall-at / --stall-for: for that window the reader keeps polling but applies nothing, the way the live members
in #4273 polled and got nothing, to show the watchdog flagging it.

Prints publish-to-reader p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and what was posted.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, PollingStrategy, SendMessage

from common import (DEFAULT_URL, DEPLOYMENT, LANE, NOTIFY, PARTITIONS, POSITIONS, PROGRESS, Stalls, Window, ensure,
                    load, msg)


async def resume_from(c, reader: str) -> dict:
    """The last position this reader published, or the current end of each topic for a new reader."""
    who, nxt, last = Consumer.Single(f"{reader}-resume-{os.getpid()}"), None, None
    while True:
        ms = await c.poll_messages(PROGRESS, POSITIONS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                   polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                   else PollingStrategy.First())
        if not ms:
            break
        for m in ms:
            d = load(m.payload())
            if d["reader"] == reader:
                last = d["pos"]
        nxt = ms[-1].offset() + 1
    if last is not None:
        out = {(k.split("/")[0], int(k.split("/")[1])): o for k, o in last.items()}
        print(f"[{reader}] resuming from its last published position ({sum(out.values()):,} notifications behind it)",
              flush=True)
        return out
    out = {}
    for t in LANE:
        topic = await c.get_topic(NOTIFY, t)
        for i, p in enumerate(sorted(topic.partitions, key=lambda p: p.id)):
            out[(t, i)] = p.current_offset + 1 if p.messages_count else 0
    print(f"[{reader}] new reader: starting at the end of each topic ({sum(out.values()):,} notifications before it)",
          flush=True)
    return out


def wants(reader: str, topic: str, d: dict) -> bool:
    if reader == "email":
        return topic != DEPLOYMENT or d["environment_slug"] == "production"
    return True


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure(await own_client(), NOTIFY, *LANE)
    await ensure(await own_client(), PROGRESS, POSITIONS)
    out = await (await own_client()).producer(PROGRESS, POSITIONS, topic_partitions_count=1,
                                              mode=DirectProducerConfig(batch_length=10,
                                                                        linger_time=timedelta(microseconds=100)))
    saved = await resume_from(await own_client(), a.reader)
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    last_seq = {}                           # (topic, key) -> last sequence posted
    posted_ids = set()
    pos = {}                                # (topic, pid) -> next offset to read
    caught_up = [False]                     # a reader first replays what is already on the log
    start = time.monotonic()
    stalled = lambda: a.stall_for and a.stall_at <= time.monotonic() - start < a.stall_at + a.stall_for
    was_stalled = [False]
    gc.freeze()                             # keep start-up objects out of the collector's way

    def post(topic, d):
        stats["posted"] += 1                # your Slack chat.postMessage or SMTP send goes here

    def on_message(topic, pid, m, now):
        d = load(m.payload())
        if m.offset() < saved.get((topic, pid), 0):
            k = (topic, d["k"])         # before our saved position: posted already, rebuild the sequence only
            last_seq[k] = max(last_seq.get(k, 0), d["seq"])
            stats["rebuilt"] += 1
            return
        age = (now - d["ns"]) / 1e6
        k = (topic, d["k"])
        last = last_seq.get(k, 0)
        if d["seq"] <= last:
            stats["dupes"] += 1             # re-delivered (at-least-once): already posted
            return
        if d["seq"] > last + 1:
            stats["missing"] += d["seq"] - last - 1
        last_seq[k] = d["seq"]
        if caught_up[0]:
            lat.add(age)
        else:
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        if wants(a.reader, topic, d):
            if d["id"] in posted_ids:
                stats["twice"] += 1
            posted_ids.add(d["id"])
            post(topic, d)

    async def read():
        c = await own_client()
        who = Consumer.Single(f"{a.reader}-{os.getpid()}")
        parts = [(t, p) for t in LANE for p in range(PARTITIONS[t])]
        while True:
            if stalled():
                was_stalled[0] = True
                await asyncio.sleep(a.poll_ms / 1000)      # polls, applies nothing
                continue
            if was_stalled[0]:
                was_stalled[0], caught_up[0] = False, False   # back: replay the backlog before timing again
            more = False
            for t, p in parts:
                ms = await c.poll_messages(NOTIFY, t, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(pos[(t, p)]) if (t, p) in pos
                                           else PollingStrategy.First())
                if ms:
                    pos[(t, p)] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_message(t, p, m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        total = defaultdict(int)
        async with out:
            while True:
                await asyncio.sleep(1)
                # position first, from this task, never from the read loop
                await out.send([SendMessage(msg(reader=a.reader, pos={f"{t}/{p}": o for (t, p), o in pos.items()}))])
                for k in ("dupes", "missing", "twice"):
                    total[k] += int(stats[k])
                tail = (f"duplicates suppressed {int(stats['dupes']):,} ({total['dupes']:,} total)  "
                        f"posted twice {total['twice']}  missing {total['missing']}")
                if stalled():
                    print(f"[{a.reader}] polling, nothing applied (stall injected at {a.stall_at:g} s for "
                          f"{a.stall_for:g} s)  {tail}", flush=True)
                elif not caught_up[0]:
                    # caught up once a whole second of notifications arrived inside the budget; partitions
                    # interleave, so one fresh notification alone doesn't mean the backlog is done
                    if stats["replayed"] and stats["max_age"] < a.budget_ms:
                        caught_up[0] = True
                    print(f"[{a.reader}] catching up: applied {int(stats['replayed']):,} queued notifications this "
                          f"second, oldest {stats['max_age'] / 1000:.1f} s  {tail}", flush=True)
                    lat.flush()
                else:
                    r = lat.flush()
                    ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
                    print(f"[{a.reader}] {r['n']:>6,} notifications/s  posted {int(stats['posted']):,}  "
                          f"publish-to-{a.reader} p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms"
                          f"  {ok} (<{a.budget_ms}ms)  {tail}  {stalls.read()}", flush=True)
                stats.clear()

    await asyncio.gather(read(), report(), stalls.watch())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--reader", choices=["slackbot", "email"], default="slackbot")
    p.add_argument("--budget-ms", type=int, default=50, help="publish-to-reader p99 budget")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--stall-at", type=float, default=0, help="seconds in when an injected stall starts")
    p.add_argument("--stall-for", type=float, default=0, help="how long the injected stall lasts (0 = none)")
    asyncio.run(main(p.parse_args()))
