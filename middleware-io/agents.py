"""The OpsAI agents: the agent in each customer cluster that runs OpsAI's kubectl commands and replies.

Plays the agents of every cluster in one process. It reads every partition of kubectl-requests directly (no
consumer group), on --readers connections of its own; each partition stands in for the agents of the clusters
that hash to it. For each request it applies the rules the agent needs to be safe when OpsAI can apply fixes:

  * Deadline first. A request carries the caller's deadline (sent time + timeout_seconds). If it has passed,
    the command is not run: the agent replies "expired". If a slot frees up only after the deadline, the same.
  * Bounded by what is left. A command that starts runs for at most the time left before the deadline, and
    is stopped at it ("timeout"), so a reply never outlives the caller's wait.
  * Off the receive loop. Commands run in tasks, at most --slots at a time per cluster (the agent's
    mcpExecSem is 8), so a slow `kubectl logs` holds one slot, not every request behind it.
  * Every request answered once. Each reply carries the request ID and a status (ok, timeout, expired),
    never an empty success. On start the agents read the reply log back --lookback-s and skip requests
    already answered, so a restarted agent does not run a fix twice.

kubectl itself is simulated (a sleep in the command's typical range from common.py); put your executor in
run_kubectl(). The offline test (--offline-partition) stops reading one partition for --offline-for seconds,
as when the agents on those clusters lose their connection or restart, then resumes where it stopped: the
requests wait in the log, and the deadline check decides which still run.

Prints a line a second: requests received, run, expired (not run), stopped at the deadline, and how many
writes a deadline-blind agent would have run after OpsAI had given up.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, PARTITIONS, REPLIES, REQUESTS, STREAM, ensure_topics, load, msg, part


async def run_kubectl(d: dict, limit_s: float) -> tuple[str, float]:
    """Stands in for kubectl in the customer's cluster: sleeps for the command's simulated time, stopped at
    limit_s. Returns (status, seconds taken). Put your executor here."""
    lo, hi = d["sim"]
    took = random.uniform(lo, hi)
    if took > limit_s:
        await asyncio.sleep(max(0.0, limit_s))
        return "timeout", limit_s
    await asyncio.sleep(took)
    return "ok", took


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), REQUESTS, REPLIES)
    # A short linger: the per-partition sends of one tick go out together, not paced apart.
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    out = await (await own_client()).producer(STREAM, REPLIES, topic_partitions_count=PARTITIONS[REPLIES], mode=cfg)
    start_ns = time.time_ns()
    since_us = start_ns // 1000 - int(a.lookback_s * 1e6)
    stats = defaultdict(int)
    pending = defaultdict(list)             # reply partition -> replies waiting for the next send
    slots = defaultdict(lambda: asyncio.Semaphore(a.slots))   # cluster -> its agent's kubectl slots
    answered = set()                        # request IDs already answered (rebuilt from the reply log)
    offline = {"on": False}
    resumed_ns = {}                         # partition -> when its agents reconnected

    # Rebuild "already answered" from the reply log, so a restarted agent never runs a request twice.
    rc = await own_client()
    for p in range(PARTITIONS[REPLIES]):
        who, strat = Consumer.Single(f"agents-rebuild-{os.getpid()}-{p}"), PollingStrategy.Timestamp(since_us)
        while True:
            ms = await rc.poll_messages(STREAM, REPLIES, consumer=who, partition_id=p, count=5000,
                                        auto_commit=False, polling_strategy=strat)
            if not ms:
                break
            for m in ms:
                answered.add(load(m.payload())["id"])
            strat = PollingStrategy.Offset(ms[-1].offset() + 1)
    print(f"[agents] rebuilt from the reply log: {len(answered):,} requests already answered "
          f"in the last {a.lookback_s:g} s", flush=True)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def reply(d, status, q_ms, x_ms, held):
        answered.add(d["id"])
        r = SendMessage(msg(id=d["id"], c=d["c"], st=status, q=round(q_ms, 3), x=round(x_ms, 1), held=held))
        pending[part(d["c"], PARTITIONS[REPLIES])].append(r)
        if random.random() < a.dup_rate:   # an agent re-sending a reply it is unsure was delivered
            pending[part(d["c"], PARTITIONS[REPLIES])].append(r)
            stats["dup_sent"] += 1

    async def execute(d, q_ms, held):
        async with slots[d["c"]]:
            left = (d["dl"] - time.time_ns()) / 1e9
            if left <= 0:                   # waited for a slot past the deadline: still not run
                stats["expired"] += 1
                stats["blind_" + d["k"]] += 1
                reply(d, "expired", q_ms, 0.0, held)
                return
            status, took = await run_kubectl(d, left - a.margin_s)
            stats["ran"] += 1
            stats["ran_" + d["k"]] += 1
            if status == "timeout":
                stats["stopped"] += 1
            if time.time_ns() > d["dl"]:
                stats["stale"] += 1         # finished after the deadline: must stay 0
            reply(d, status, q_ms, took * 1000, held)

    def on_request(m, now, p):
        d = load(m.payload())
        stats["received"] += 1
        if d["id"] in answered:
            stats["skipped"] += 1           # answered before a restart: do not run it again
            return
        q_ms = (now - d["ns"]) / 1e6
        # waited in the log: sent while this partition's agents were disconnected, or replayed after a restart
        held = q_ms > 1000 or d["ns"] < resumed_ns.get(p, 0)
        if held:
            stats["held"] += 1
        if now >= d["dl"]:                  # deadline passed before the agent saw it: never run
            stats["expired"] += 1
            stats["blind_" + d["k"]] += 1   # what a deadline-blind agent would have run here
            reply(d, "expired", q_ms, 0.0, held)
            return
        asyncio.get_running_loop().create_task(execute(d, q_ms, held))

    async def read(pids):
        # Each partition in turn, from --lookback-s ago (older requests are past any deadline), then from our
        # own position; nothing is stored on the broker. A full batch means more is waiting.
        c = await own_client()
        who = {p: Consumer.Single(f"agents-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                if offline["on"] and p == a.offline_partition:
                    continue                # this partition's agents are disconnected; requests wait in the log
                ms = await c.poll_messages(STREAM, REQUESTS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.Timestamp(since_us))
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_request(m, now, p)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_replies():
        # Replies are small and bursty; one batch per partition every 2 ms, sent together, keeps the reader
        # loop free of sends and each cluster's replies in order.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batches = dict(pending)
                    pending.clear()
                    await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                    stats["sent"] += sum(len(ms) for ms in batches.values())

    async def outage():
        if a.offline_partition < 0:
            return
        await asyncio.sleep(a.offline_at)
        offline["on"] = True
        print(f"[agents] partition {a.offline_partition}: agents disconnected for {a.offline_for:g} s", flush=True)
        await asyncio.sleep(a.offline_for)
        resumed_ns[a.offline_partition] = time.time_ns()
        offline["on"] = False
        print(f"[agents] partition {a.offline_partition}: agents back, reading on from where they stopped", flush=True)

    async def report():
        total = defaultdict(int)
        while True:
            await asyncio.sleep(1)
            for k, v in stats.items():
                total[k] += v
            print(f"[agents] {stats['received']:>6,} requests/s  run {stats['ran']:,} "
                  f"(writes {stats['ran_write']:,})  expired, not run {stats['expired']:,} "
                  f"(writes {stats['blind_write']:,})  stopped at deadline {stats['stopped']:,}  "
                  f"finished after deadline {stats['stale']:,}  held in the log {stats['held']:,}  "
                  f"skipped, already answered {stats['skipped']:,}  replies sent {stats['sent']:,}  "
                  f"{'OFFLINE p%d' % a.offline_partition if offline['on'] else ''}", flush=True)
            print(f"[agents] totals: run {total['ran']:,}  expired, not run {total['expired']:,}  "
                  f"a deadline-blind agent would have run {total['blind_read'] + total['blind_write']:,} stale "
                  f"commands, {total['blind_write']:,} of them writes  finished after deadline {total['stale']:,}",
                  flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[REQUESTS]))
    await asyncio.gather(send_replies(), report(), outage(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the request partitions")
    p.add_argument("--slots", type=int, default=8, help="kubectl commands at once per cluster (mcpExecSem)")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--margin-s", type=float, default=0.2, help="stop a command this long before its deadline")
    p.add_argument("--lookback-s", type=float, default=300, help="read back this far on start (>= longest timeout)")
    p.add_argument("--dup-rate", type=float, default=0.002, help="share of replies sent twice")
    p.add_argument("--offline-partition", type=int, default=-1, help="partition whose agents disconnect (-1: none)")
    p.add_argument("--offline-at", type=float, default=30, help="seconds after start")
    p.add_argument("--offline-for", type=float, default=25, help="seconds")
    asyncio.run(main(p.parse_args()))
    os._exit(0)
