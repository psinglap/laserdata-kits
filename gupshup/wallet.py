"""The wallet service, fed by the charges lane.

Reads the charges topic directly, partition by partition, on --readers connections of its own (no consumer
group: a group reader visits one partition per poll, so a record waits for its partition's turn). Each record
is one send node's running totals for one app (messages accepted, Gupshup fee, WhatsApp fee) or the app's
running top-ups, with a sequence. An app's balance is its opening balance plus top-ups minus the latest totals
from every node, so a record sent twice, or replayed after a restart, changes nothing.

For every app it decides, as charges arrive:
  * LOW once the balance falls under the low-balance threshold (in production, the $5 alert email);
  * BLOCKED the moment the balance reaches the app's overdraft limit: send nodes reject with 1003;
  * OK when a top-up brings the balance back above the threshold;
and publishes each change on wallet-state, which every send API node reads (send_api.py).

Prints charge-to-wallet p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business
number: messages accepted past the overdraft, the messages sent after an app had already used up its
balance and its overdraft, because the charge or the block was still in flight.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import CHARGES, DEFAULT_URL, STATE, STREAM, Window, app, ensure_topics, load, msg, wallet


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), CHARGES, STATE)
    out = await (await own_client()).producer(STREAM, STATE, topic_partitions_count=1)
    info = {app(i): wallet(i, a.apps, a.msgs_per_s, a.runway_s) for i in range(a.apps)}
    bal = {x: w["opening"] for x, w in info.items()}
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (node, app) -> last sequence applied
    last = {}                               # (node, app) -> that node's latest [messages, gs, wa, credits]
    state = {}                              # app -> "low" | "blocked" (absent = ok)
    dseq = defaultdict(int)                 # app -> state sequence
    pending = []                            # state changes waiting for the next send (one batch per tick)
    run = defaultdict(float)                # totals since start: accepted, past, blocks, recharges, wa_after
    caught_up = [False]                     # a new reader first replays charges from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def decide(x, new):
        if state.get(x, "ok") == new:
            return
        if new == "ok":
            state.pop(x, None)
        else:
            state[x] = new
        dseq[x] += 1
        pending.append(SendMessage(msg(a=x, seq=dseq[x], state=new, bal=round(bal[x], 4))))
        stats[new] += 1

    def on_charge(m):
        c = load(m.payload())
        now = time.time_ns()
        age = (now - c["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = (c["n"], c["a"])
        if c["seq"] <= seq.get(k, 0):
            stats["repeats"] += 1           # sent twice or replayed: already charged
            return
        seq[k] = c["seq"]
        x = c["a"]
        if x not in info:                   # an app this run doesn't know (a shadow run): the default wallet
            info[x] = {"opening": a.default_balance, "overdraft": a.default_overdraft, "low": a.default_low}
            bal[x] = a.default_balance
        old = last.get(k, (0, 0.0, 0.0, 0.0))
        dm, dgs, dwa, dcr = c["m"] - old[0], c["gs"] - old[1], c["wa"] - old[2], c["cr"] - old[3]
        last[k] = (c["m"], c["gs"], c["wa"], c["cr"])
        w = info[x]
        over = bal[x] <= -w["overdraft"]    # already at the overdraft limit before this record
        bal[x] += dcr - dgs - dwa
        stats["events"] += 1
        run["accepted"] += dm
        if over:
            run["past"] += dm               # accepted after the balance and overdraft were used up
            run["wa_after"] += dwa          # Meta fees for messages delivered after the block (mostly sent before it)
        if dcr > 0:
            run["recharges"] += 1
        if bal[x] <= -w["overdraft"]:
            if state.get(x) != "blocked":
                run["blocks"] += 1
            decide(x, "blocked")
        elif bal[x] < w["low"]:
            decide(x, "low")                # also after a top-up too small to clear the threshold
        else:
            decide(x, "ok")

    async def send_states():
        # States are small and come in bursts; one batch every 2 ms keeps them off a send-per-state loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (balances are rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, CHARGES, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_charge(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of charges arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier charge records this second",
                      flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} charges/s  charge-to-wallet p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"states sent {int(stats['sent']):,}", flush=True)
            acc = run["accepted"]
            print(f"[ledger] apps blocked {sum(1 for v in state.values() if v == 'blocked'):,} "
                  f"low {sum(1 for v in state.values() if v == 'low'):,} of {a.apps:,}  "
                  f"blocks {int(run['blocks']):,}  recharges {int(run['recharges']):,}  "
                  f"repeats ignored {int(stats['repeats']):,}  "
                  f"accepted past the overdraft {int(run['past']):,} of {int(acc):,} msgs "
                  f"({100 * run['past'] / acc if acc else 0:.4f}%)  Meta fees after block ${run['wa_after']:,.4f}",
                  flush=True)
            for k in ("events", "sent", "repeats", "low", "blocked", "ok"):
                stats[k] = 0

    parts = list(range((await (await own_client()).get_topic(STREAM, CHARGES)).partitions_count))
    await asyncio.gather(send_states(), report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the charges partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="wallet")
    p.add_argument("--budget-ms", type=int, default=50, help="charge-to-wallet p99 budget for the lane")
    p.add_argument("--apps", type=int, default=5_000, help="must match send_api.py")
    p.add_argument("--msgs-per-s", type=float, default=4_000, help="must match send_api.py (sets the balances)")
    p.add_argument("--runway-s", type=float, default=30, help="must match send_api.py")
    p.add_argument("--default-balance", type=float, default=100, help="opening USD for apps not in this run (shadow)")
    p.add_argument("--default-overdraft", type=float, default=20, help="overdraft USD for apps not in this run")
    p.add_argument("--default-low", type=float, default=5, help="low-balance alert USD for apps not in this run")
    asyncio.run(main(p.parse_args()))
