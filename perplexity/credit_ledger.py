"""The credit ledger for Computer, fed by the agent-actions lane.

Reads the agent-actions topic directly, partition by partition, on --readers connections of its own (no
consumer group: a group reader visits one partition per poll, so with 16 partitions a record would wait for its
partition's turn). Each record is one agent runner's running credits for one account (the resulting total,
with a sequence), so an account's spend is the sum of the latest totals from every runner, and replays or
repeats change nothing.

For every account it keeps the balance in Perplexity's order: bonus credits first, then monthly ("plan"), then
purchased. As credits arrive it decides:
  * AUTO-REFILL when the balance falls below the account's threshold, if refill is on and the monthly spending
    cap allows it; the purchased credits land after --charge-ms (the card charge, illustrative);
  * PAUSE the moment the balance reaches zero: that account's tasks pause, work saved, no new actions start;
  * RESUME when credits are back: a refill lands or the monthly credits reset;
and publishes each pause and resume on credit-decisions, which every agent runner reads (agent_runners.py).

Prints action-to-ledger p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business number:
credits past zero, the agent actions run for an account after its balance had already reached zero, because the
action or the pause was still in flight. (The action that spends the last credits is let finish; its remainder
is written off and not counted, since no lane could have stopped it.)
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (ACTIONS, CREDITS_PER_USD, DECISIONS, DEFAULT_URL, PARTITIONS, STREAM, Window, account,
                    ensure_topics, load, msg, period, plan)


class Acct:
    __slots__ = ("plan", "period", "bonus", "monthly", "bought", "refilled", "refill_due", "paused")

    def __init__(self, pl):
        self.plan, self.period = pl, -1
        self.bonus, self.monthly, self.bought = pl["bonus"], 0, 0
        self.refilled, self.refill_due, self.paused = 0, 0.0, False

    def balance(self):
        return self.bonus + self.monthly + self.bought


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ACTIONS, DECISIONS)
    out = await (await own_client()).producer(STREAM, DECISIONS, topic_partitions_count=PARTITIONS[DECISIONS])
    accts = {account(i): Acct(plan(account(i), a.actions_per_s, a.accounts, a.period_s)) for i in range(a.accounts)}
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (runner, account) -> last sequence applied
    last = {}                               # (runner, account) -> that runner's latest running total
    dseq = defaultdict(int)                 # account -> decision sequence
    pending = []                            # decisions waiting for the next send (one batch per tick)
    spent = defaultdict(lambda: defaultdict(float))   # period -> bucket -> credits spent
    past = defaultdict(float)               # period -> credits spent after the balance had reached zero
    count = defaultdict(int)                # (period, what) -> pauses, resumes, refills
    due = {}                                # account -> time its auto-refill charge clears
    paused = set()                          # accounts paused now
    caught_up = [False]                     # a new reader first replays actions from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def decide(u, st, action, why):
        st.paused = action == "pause"
        (paused.add if st.paused else paused.discard)(u)
        dseq[u] += 1
        pending.append(SendMessage(msg(a=u, seq=dseq[u], action=action, why=why)))
        count[(period(a.period_s), action)] += 1

    def roll(st, p):
        # A new month for this account: monthly credits reset (they don't carry over), refills count afresh.
        if p > st.period:
            st.period, st.monthly, st.refilled = p, st.plan["monthly"], 0

    def maybe_refill(u, st, now):
        pl = st.plan
        if (pl["refill"] and not st.refill_due and st.balance() < pl["threshold"]
                and st.refilled + pl["refill"] <= pl["cap"]):
            st.refill_due = due[u] = now + a.charge_ms / 1000

    def on_action(m):
        r = load(m.payload())
        now = time.time_ns()
        age = (now - r["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = (r["s"], r["a"])
        if r["seq"] <= seq.get(k, 0):
            stats["dropped"] += 1           # older or repeated total: the newest wins
            return
        seq[k] = r["seq"]
        u, p = r["a"], r["p"]
        st = accts[u]
        roll(st, p)
        cost = r["cr"] - last.get(k, 0)
        last[k] = r["cr"]
        stats["events"] += 1
        empty = st.balance() <= 0
        for b in ("bonus", "monthly", "bought"):        # Perplexity's order: bonus, then monthly, then purchased
            take = min(cost, getattr(st, b))
            if take:
                setattr(st, b, getattr(st, b) - take)
                spent[p][b] += take
                cost -= take
        if cost:
            spent[p]["over"] += cost        # the action that spends the last credits finishes; its remainder is written off
            if empty:                       # the balance was already zero: an action started after the credits ran out,
                past[p] += cost             # because this action or the pause was still in flight
        maybe_refill(u, st, now / 1e9)
        if st.balance() <= 0 and not st.paused:
            decide(u, st, "pause", "no credits")

    async def housekeeping():
        # Refills land when their charge clears; months roll over for paused accounts, which send nothing.
        p_seen = period(a.period_s)
        while True:
            await asyncio.sleep(0.005)
            now = time.time()
            p = period(a.period_s, now)
            for u in [u for u, t in due.items() if now >= t]:
                del due[u]
                st = accts[u]
                st.refill_due = 0.0
                st.bought += st.plan["refill"]
                st.refilled += st.plan["refill"]
                count[(p, "refill")] += 1
                if st.paused and st.balance() > 0:
                    decide(u, st, "resume", "auto-refill")
            if p != p_seen:
                for u in list(paused):
                    st = accts[u]
                    roll(st, p)
                    if st.balance() > 0:
                        decide(u, st, "resume", "monthly reset")
                    else:
                        maybe_refill(u, st, now)
                p_seen = p

    async def send_decisions():
        # Decisions are small and come in bursts; one batch every 2 ms keeps them off a send-per-decision loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["decisions"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (balances are rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, ACTIONS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_action(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of actions arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier action records this second",
                      flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            p = period(a.period_s)
            sp = spent[p]
            used = sp["bonus"] + sp["monthly"] + sp["bought"] + sp["over"]
            by_plan = defaultdict(int)
            for u in paused:
                by_plan[accts[u].plan["plan"]] += 1
            print(f"[{a.name}] {r['n']:>7,} action records/s  action-to-ledger p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"decisions sent {int(stats['decisions']):,}", flush=True)
            print(f"[credits] month {p % 10000}: spent {used:,.0f} (bonus {sp['bonus']:,.0f} plan {sp['monthly']:,.0f} "
                  f"purchased {sp['bought']:,.0f})  refills {count[(p, 'refill')]:,}  pauses {count[(p, 'pause')]:,}  "
                  f"resumes {count[(p, 'resume')]:,}  paused now {len(paused):,} (pro {by_plan['pro']:,} "
                  f"max {by_plan['max']:,} enterprise {by_plan['enterprise']:,})  past zero {past[p]:,.0f} credits "
                  f"${past[p] / CREDITS_PER_USD:,.2f} ({100 * past[p] / used if used else 0:.3f}% of credits)", flush=True)
            stats.clear()
            for d in (spent, past):
                for k in [k for k in d if k <= p - 3]:
                    del d[k]

    parts = list(range((await (await own_client()).get_topic(STREAM, ACTIONS)).partitions_count))
    await asyncio.gather(send_decisions(), report(), housekeeping(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the agent-actions partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="ledger")
    p.add_argument("--budget-ms", type=int, default=50, help="action-to-ledger p99 budget for the lane")
    p.add_argument("--accounts", type=int, default=20_000, help="must match agent_runners.py")
    p.add_argument("--actions-per-s", type=float, default=10_000, help="must match agent_runners.py (sets the plans)")
    p.add_argument("--period-s", type=float, default=60, help="seconds per compressed month; must match agent_runners.py")
    p.add_argument("--charge-ms", type=float, default=500, help="time for an auto-refill charge to clear (illustrative)")
    asyncio.run(main(p.parse_args()))
