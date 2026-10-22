"""The shared plan-ceiling ledger, fed by the concurrency lane.

Reads the slot-events topic directly, partition by partition, on --readers connections of its own (no consumer
group: a group reader visits one partition per poll, so a record waited for its partition's turn). Each record is
one voice-agent worker's live conversations for one organization (the resulting count, with a sequence), so an
organization's live conversations are the sum of the latest counts from every worker, and replays or repeats
change nothing. Workers also send a beat once a second; a worker silent for LEASE_S seconds has its slots
released, so a crashed worker cannot hold an organization's capacity.

For every organization it compares live conversations with the plan ceiling, which is shared by every campaign,
instant outbound and inbound call, with part of it kept for inbound:
  * OPEN           below the outbound share: campaigns and instant outbound may start conversations;
  * OUTBOUND_FULL  outbound has used its share: only inbound calls start, into the reserved capacity;
  * FULL           at the ceiling: new calls are queued or rejected;
and publishes each change on org-headroom, which every worker reads (call_workers.py).

Prints slot-event-to-ledger p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the business
number: conversations that started past a ceiling (outbound into the inbound reserve, or anything past the plan
ceiling) because a start or a headroom change was still in flight.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (DEFAULT_URL, FULL, HEADROOM, LEASE_S, OPEN, OUT_FULL, SLOTS, STREAM, Window, ensure_topics,
                    load, msg, org, plan, state_of)


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SLOTS, HEADROOM)
    out = await (await own_client()).producer(STREAM, HEADROOM, topic_partitions_count=1)
    orgs = [org(i) for i in range(a.orgs)]
    plans = {o: plan(o, a.conv_per_s, a.talk_s, a.orgs) for o in orgs}
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (worker, org) -> last sequence applied
    held = {}                               # (worker, org) -> that worker's latest live count
    live = defaultdict(int)                 # org -> live conversations: sum of every worker's latest count
    seen = {}                               # worker -> ns of its latest record or beat
    state = {}                              # org -> open | outbound_full | full
    dseq = defaultdict(int)                 # org -> decision sequence
    pending = []                            # decisions waiting for the next send (one batch per tick)
    caught_up = [False]                     # a new reader first replays slot events from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def decide(o):
        ceiling, reserve = plans[o]
        new = state_of(live[o], ceiling, reserve)
        if state.get(o, OPEN) == new:
            return
        state[o] = new
        dseq[o] += 1
        pending.append(SendMessage(msg(o=o, seq=dseq[o], state=new, live=live[o])))

    def on_slot(m):
        s = load(m.payload())
        now = time.time_ns()
        age = (now - s["ns"]) / 1e6
        w = s["w"]
        seen[w] = max(seen.get(w, 0), s["ns"])
        if "beat" in s:
            return
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest record seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = (w, s["o"])
        if s["seq"] <= seq.get(k, 0):
            stats["dropped"] += 1           # older or repeated count: the newest wins
            return
        seq[k] = s["seq"]
        o = s["o"]
        before = live[o]
        live[o] += s["live"] - held.get(k, 0)
        held[k] = s["live"]
        stats["events"] += 1
        if s["ev"] == "start" and caught_up[0]:
            ceiling, reserve = plans[o]
            stats["starts"] += 1
            if s["src"] != "inbound":
                stats["out_starts"] += 1
                if before >= ceiling - reserve:
                    stats["out_past"] += 1  # an outbound conversation started inside the inbound reserve
            if before >= ceiling:
                stats["past_ceiling"] += 1  # any conversation started past the plan ceiling
        decide(o)

    def expire():
        # Release the slots of workers that went silent: the lease, not a manual reset, frees their capacity.
        cut = time.time_ns() - LEASE_S * 1e9
        for w in [w for w, t in seen.items() if t < cut]:
            del seen[w]
            for k in [k for k in held if k[0] == w]:
                live[k[1]] -= held.pop(k)
                stats["released"] += 1
                decide(k[1])

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
        # Each partition in turn, from the start of the log (live counts are rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, SLOTS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_slot(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            expire()
            if not caught_up[0]:
                # caught up once a whole second of slot events arrived inside the budget; partitions interleave,
                # so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier slot events this second",
                      flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>6,} slot events/s  slot-event-to-ledger p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"decisions sent {int(stats['decisions']):,}", flush=True)
            out_s = stats["out_starts"]
            print(f"[ceiling] live conversations {sum(live.values()):,}  orgs outbound-full "
                  f"{sum(1 for v in state.values() if v == OUT_FULL):,}  full {sum(1 for v in state.values() if v == FULL):,}"
                  f"/{a.orgs:,}  starts {int(stats['starts']):,}  outbound past its share {int(stats['out_past']):,} "
                  f"({100 * stats['out_past'] / out_s if out_s else 0:.2f}% of outbound starts)  "
                  f"past the plan ceiling {int(stats['past_ceiling']):,}  slots released by lease "
                  f"{int(stats['released']):,}", flush=True)
            stats.clear()

    parts = list(range((await (await own_client()).get_topic(STREAM, SLOTS)).partitions_count))
    await asyncio.gather(send_decisions(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the slot-event partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="ledger")
    p.add_argument("--budget-ms", type=int, default=50, help="slot-event-to-ledger p99 budget for the lane")
    p.add_argument("--orgs", type=int, default=300, help="must match call_workers.py")
    p.add_argument("--conv-per-s", type=float, default=60, help="must match call_workers.py (sets the ceilings)")
    p.add_argument("--talk-s", type=float, default=90, help="average talk time in seconds; must match call_workers.py")
    asyncio.run(main(p.parse_args()))
