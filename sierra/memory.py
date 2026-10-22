"""The memory builder, fed by the turns lane.

Reads every partition of the turns topic directly (no consumer group: a group reader visits one partition per
poll, so with 16 partitions a turn waited for its partition's turn and the tail grew on a busy host), on
--readers connections of its own, tracking its own position. Each record is one turn of one customer's
conversation, on any channel, with the customer's turn sequence, so a replayed or repeated turn is recognised
by its sequence and changes nothing.

For every turn it runs extract() (a stand-in for your memory extraction) and, when the turn adds a fact,
publishes the customer's updated memory on customer-memory, keyed by customer, for the store that every
channel's agent reads at its next message (agents.py plays that store). Each record carries the whole
resulting memory (the latest facts, the fact count and the turn sequence it is current to), never only the new
fact, so a store that misses or repeats a record converges on the next one, and a restarted builder rebuilds
every customer's memory from the log.

Prints turn-to-memory p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, MEMORY, PARTITIONS, STREAM, TURNS, Window, ensure_topics, load, msg, part


def extract(turn: dict) -> str | None:
    """Stands in for your memory extraction (a model call, rules, or both). Here a turn that adds a fact carries
    it in the record; put your extraction here, in-process or as a batched call to your model servers."""
    return turn.get("fact")


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), TURNS, MEMORY)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    out = await (await own_client()).producer(STREAM, MEMORY, topic_partitions_count=PARTITIONS[MEMORY], mode=cfg)
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # customer -> last turn sequence applied
    facts = defaultdict(lambda: deque(maxlen=a.keep))   # customer -> latest facts, newest last
    count = defaultdict(int)                # customer -> facts so far
    pending = defaultdict(list)             # memory partition -> records waiting for the next send (every 2 ms)
    caught_up = [False]                     # a new reader first replays turns from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_turn(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest turn seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = d["k"]
        if d["seq"] <= seq.get(k, 0):
            stats["dropped"] += 1           # older or repeated turn: already applied
            return
        seq[k] = d["seq"]
        stats["turns"] += 1
        fact = extract(d)
        if fact is None:
            return
        facts[k].append(fact)
        count[k] += 1
        stats["facts"] += 1
        if age > a.replay_publish_s * 1000:
            return                          # replayed from long ago: memory rebuilt, the store already has it
        pending[part(k, PARTITIONS[MEMORY])].append(
            SendMessage(msg(k=k, v=d["seq"], ch=d["ch"], n=count[k], facts=list(facts[k]))))

    async def read(pids):
        # Each partition in turn, from the start of the log (memory is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, TURNS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_turn(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_memory():
        # Memory updates are small and bursty; one batch per partition every 2 ms, all sent at once, keeps them
        # off a send-per-update loop and keeps each customer's updates in order.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batches = dict(pending)
                    pending.clear()
                    await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                    stats["sent"] += sum(len(ms) for ms in batches.values())

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of turns arrived inside the budget; partitions interleave,
                # so one fresh turn alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier turns this second, "
                      f"memory rebuilt for {len(count):,} customers", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} turns/s  turn-to-memory p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  facts {int(stats['facts']):,}  "
                  f"memory updates sent {int(stats['sent']):,}  customers with memory {len(count):,}", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[TURNS]))
    await asyncio.gather(send_memory(), report(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="memory")
    p.add_argument("--budget-ms", type=int, default=50, help="turn-to-memory p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the turns partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--keep", type=int, default=8, help="latest facts kept per customer in each memory record")
    p.add_argument("--replay-publish-s", type=float, default=30,
                   help="after a restart, still publish memory for turns younger than this")
    asyncio.run(main(p.parse_args()))
