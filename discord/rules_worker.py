"""A rules worker: owns a fixed set of partitions of the actions topic and evaluates each action against
the rules, per user, in order.

Worker --worker of --workers owns every partition p with p % workers == worker and reads each one
directly (no consumer group, no rebalance), in order. Because the action producers key by user, every
action of a user reaches this one worker, one after another: when a rule labels a user, that label is
in place before the user's next action is evaluated, with no race against another worker. Offsets are
stored on the broker at most once a second under a per-partition consumer name, so whichever worker
owns a partition next resumes from it (actions after the last stored offset are read again; the
per-user sequence drops them).

The rules are deliberately small and stand in for Osprey's SML rules (Discord's are its own):
  * NewAccountLinkBurst: an account younger than --new-account-h hours that sends --burst messages with
    links inside --window-s seconds gets LabelAdd(UserId, 'likely_spammer') and that message is rejected;
  * LabelledSender: every later send_message from a labelled user gets DeclareVerdict('reject') and a
    delete_message effect.
Every execution result (verdict, label added, whether the label was in place) is written to the
execution-results topic, keyed by user, with this worker's ID, by a separate sender every --flush-ms.

Prints a line a second:
  * action-to-verdict latency (written by the producer to evaluated here), PASS or FAIL against --budget-ms;
  * labels added, and messages rejected because the label was already in place;
  * order violations, missing actions, and users whose actions came in on another partition
    (all three are 0 when actions are keyed by user).
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, PollingStrategy, SendMessage, Partitioning

from common import (ACTIONS_TOPIC, DEFAULT_URL, RESULTS_TOPIC, SPAM_LABEL, STREAM, Window, connect, ensure_topic,
                    load, msg, owner, part)


async def main(a):
    name = f"rules-{a.worker}"
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, ACTIONS_TOPIC, a.partitions)     # whoever starts first creates the topics
    await ensure_topic(admin, STREAM, RESULTS_TOPIC, a.result_partitions)
    parts = (await admin.get_topic(STREAM, ACTIONS_TOPIC)).partitions_count
    res_parts = (await admin.get_topic(STREAM, RESULTS_TOPIC)).partitions_count
    mine = [p for p in range(parts) if owner(p, a.workers) == a.worker]
    budget_ns = a.budget_ms * 1_000_000
    window_ns = int(a.window_s * 1e9)
    last_seq, seen_on, recent, labels = {}, {}, {}, set()
    lat = Window()
    st, total = defaultdict(int), defaultdict(int)
    caught_up = [False]                                   # a new reader first replays what was written before it
    gc.freeze()                                           # keep start-up objects out of the collector's way

    def evaluate(e, now, out):
        u, seq = e["u"], e["seq"]
        if "x" not in e:                                  # keyed: one partition per user, in order
            if seen_on.setdefault(u, e["_p"]) != e["_p"]:
                total["moved"] += 1
            last = last_seq.get(u, 0)
            if seq <= last:
                total["out_of_order" if seq < last else "repeats"] += 1
                return
            if seq > last + 1 and last:
                total["lost"] += seq - last - 1
            last_seq[u] = seq
        verdict, added, had = "allow", 0, int(u in labels)
        if e["a"] == "send_message":
            if had:
                verdict = "reject"                        # LabelledSender: delete_message
                st["rejected"] += 1
            elif e["link"] and e["age_h"] < a.new_account_h:
                q = recent.setdefault(u, deque())
                q.append(e["ns"])
                while q and e["ns"] - q[0] > window_ns:
                    q.popleft()
                if len(q) >= a.burst:                     # NewAccountLinkBurst: LabelAdd likely_spammer
                    labels.add(u)
                    recent.pop(u, None)
                    verdict, added = "reject", 1
                    st["labels"] += 1
        out[part(u, res_parts)].append(SendMessage(msg(
            ns=e["ns"], ens=now, u=u, a=e["a"], seq=seq, v=verdict, lbl=SPAM_LABEL if added else "",
            had=had, w=a.worker)))
        age = now - e["ns"]
        if not caught_up[0]:
            st["replayed"] += 1                           # written before this worker started: not live latency
            st["max_age"] = max(st["max_age"], age)
        else:
            lat.add(age / 1e6)
        st["actions"] += 1

    out = defaultdict(list)                               # execution results waiting for the next flush

    async def reader(plist):
        c = await connect(a.url)
        consumers = {p: Consumer.Single(f"osprey-rules-p{p}") for p in plist}
        nxt, committed = {}, {}
        while True:
            more = False
            for p in plist:
                start = PollingStrategy.Offset(nxt[p]) if p in nxt else PollingStrategy.Next()
                # Store this partition's offset on the broker at most once a second, not on every poll.
                commit = time.monotonic() - committed.get(p, 0) >= a.commit_s
                ms = await c.poll_messages(STREAM, ACTIONS_TOPIC, consumer=consumers[p], partition_id=p,
                                           count=a.batch, auto_commit=commit, polling_strategy=start)
                if ms:
                    if commit:
                        committed[p] = time.monotonic()
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        e = load(m.payload())
                        e["_p"] = p
                        evaluate(e, now, out)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def sender():
        # Execution results go out on their own connection, one batch per results partition per tick,
        # so the reader never waits on a send and the broker sees a few larger writes, not many tiny ones.
        w = await connect(a.url)
        while True:
            await asyncio.sleep(a.flush_ms / 1000)
            if out:
                batch = dict(out)
                out.clear()
                await asyncio.gather(*(w.send_messages(STREAM, RESULTS_TOPIC, Partitioning.partition_id(pid), ms)
                                       for pid, ms in batch.items()))

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of actions arrived inside the budget; partitions interleave,
                # so one fresh action alone doesn't mean the replay is finished
                if st["replayed"] and st["max_age"] < budget_ns:
                    caught_up[0] = True
                print(f"[{name}] catching up: replayed {st['replayed']:,} earlier actions this second", flush=True)
                st.clear()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{name}] {r['n']:>6,} actions/s evaluated  action-to-verdict p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"labels added {st['labels']:>4,}/s  rejected with label in place {st['rejected']:>5,}/s  "
                  f"out of order {total['out_of_order']:,}  missing {total['lost']:,}  "
                  f"on another partition {total['moved']:,}  users {len(last_seq):,}", flush=True)
            st.clear()

    groups = [mine[r::a.readers] for r in range(a.readers)]
    print(f"[{name}] owns partitions {mine} of {parts} (worker {a.worker} of {a.workers}), "
          f"reading on {a.readers} connection(s)", flush=True)
    await asyncio.gather(report(), sender(), *(reader(g) for g in groups if g))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--worker", type=int, default=0)
    p.add_argument("--workers", type=int, default=4, help="rules workers splitting the partitions")
    p.add_argument("--partitions", type=int, default=64, help="partitions for a new actions topic")
    p.add_argument("--result-partitions", type=int, default=16, help="partitions for a new execution-results topic")
    p.add_argument("--readers", type=int, default=1, help="connections reading this worker's partitions")
    p.add_argument("--new-account-h", type=float, default=24, help="accounts younger than this count as new")
    p.add_argument("--burst", type=int, default=5, help="messages with links from a new account that trigger the label")
    p.add_argument("--window-s", type=float, default=10, help="inside this many seconds")
    p.add_argument("--budget-ms", type=float, default=50, help="p99 budget, action written to verdict")
    p.add_argument("--poll-ms", type=float, default=5, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--flush-ms", type=float, default=20, help="how often execution results are sent")
    p.add_argument("--commit-s", type=float, default=1, help="how often each partition's offset is stored")
    asyncio.run(main(p.parse_args()))
