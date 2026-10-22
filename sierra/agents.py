"""Agent runtimes on every channel: every turn onto the lane, and each customer's memory checked at a channel switch.

Plays --runtimes-per-channel agent runtimes for each of voice, chat and SMS in one process. Customers talk at
illustrative gaps (common.GAP_S) and, on a share of turns (--switch-rate), carry on on another channel: they
open the link the agent just texted during a call, move from the app to a call, or reply by SMS
(common.switch_gap). Each turn lands on any runtime of its channel, as a load balancer would send it. For every
turn the runtime publishes to the turns topic, keyed by customer:
  * customer, channel, runtime, the customer's turn sequence, the turn text, and the fact it adds to the
    customer's memory, if any (a share of turns do, --fact-share).
The process also plays the memory store every runtime reads (one direct reader of customer-memory, on its own
connection): it keeps each customer's memory at the newest turn sequence it has seen.

The business number: of the channel switches by customers who already told the agent something, how many found
the store missing the latest fact, so the agent on the new channel would ask again. Next to it, how many quick
switches carried a fact from the conversation still open on the other channel: a memory written only when a
conversation ends would miss those. Prints both every second, with memory-to-store p50/p99: the second hop.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (CHANNELS, DEFAULT_URL, FACTS, MEMORY, PARTITIONS, STREAM, TURNS, Window, customer,
                    ensure_topics, load, mean_gap, msg, next_gap, part, pick_channel, switch_gap)

# Stands in for one turn of the transcript; the real lane carries your turn text.
FILLER = ("Customer: the box was crushed when it arrived and the screen has a crack across it. Agent: I'm sorry "
          "about that. I can send a replacement today or refund it, which would you prefer? ") * 20


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), TURNS, MEMORY)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, TURNS, topic_partitions_count=PARTITIONS[TURNS], mode=cfg)
    start_ns = time.time_ns()
    runtimes = {ch: [f"{ch}-{i:02d}" for i in range(a.runtimes_per_channel)] for ch in CHANNELS}
    n_cust = max(1, int(a.turns_per_s * mean_gap()))
    custs = [customer(i) for i in range(n_cust)]
    pid = [part(k, PARTITIONS[TURNS]) for k in custs]
    ch = [pick_channel() for _ in range(n_cust)]
    switch_to = [None] * n_cust             # the channel a customer's next turn moves to, if it moves
    quick = [False] * n_cust                # that move comes while the conversation it leaves is still open
    seq = [0] * n_cust                      # customer -> turn sequence
    stint = [0] * n_cust                    # turn sequence when the customer's current channel began
    last_fact = {}                          # customer index -> turn sequence of the customer's latest fact
    now = time.monotonic()
    due = [(now + random.uniform(0, next_gap(ch[i])), i) for i in range(n_cust)]
    heapq.heapify(due)
    store = {}                              # the memory store: customer -> turn sequence its memory is current to
    lat, stats = Window(), defaultdict(float)

    async def store_reader():
        # The memory store: every customer-memory partition read directly, from our own position.
        c = await own_client()
        pids = list(range(PARTITIONS[MEMORY]))
        who = {p: Consumer.Single(f"store-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, MEMORY, consumer=who[p], partition_id=p, count=5000,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    t = time.time_ns()
                    for m in ms:
                        d = load(m.payload())
                        if d["v"] > store.get(d["k"], 0):   # older or repeated record: the newest memory wins
                            store[d["k"]] = d["v"]
                        if d["ns"] > start_ns:
                            lat.add((t - d["ns"]) / 1e6)    # live updates only, not ones replayed at start-up
                    more = more or len(ms) == 5000
            if not more:
                await asyncio.sleep(0.001)

    gc.freeze()                             # keep start-up objects out of the collector's way

    async def turn_loop():
        t0 = tick = time.monotonic()
        async with producer:
            while True:
                now = time.monotonic()
                x = a.peak if a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1
                batches = defaultdict(list)
                while due and due[0][0] <= now:
                    _, i = heapq.heappop(due)
                    k = custs[i]
                    if switch_to[i]:        # first message on the new channel: is the memory current?
                        ch[i], switch_to[i] = switch_to[i], None
                        stats["switches"] += 1
                        f = last_fact.get(i)
                        if f is not None:
                            stats["with_memory"] += 1
                            if store.get(k, 0) < f:
                                stats["ask_again"] += 1     # the store is missing the customer's latest fact
                            if quick[i] and f > stint[i]:
                                stats["open_conv"] += 1     # that fact came from the conversation still open
                        stint[i] = seq[i]
                    seq[i] += 1
                    fact = None
                    if random.random() < a.fact_share:
                        fact = random.choice(FACTS)
                        last_fact[i] = seq[i]
                        stats["facts"] += 1
                    off = random.randrange(200)
                    batches[pid[i]].append(SendMessage(msg(k=k, ch=ch[i], rt=random.choice(runtimes[ch[i]]),
                                                           seq=seq[i], fact=fact,
                                                           text=FILLER[off:off + a.turn_bytes])))
                    stats["turns"] += 1
                    if random.random() < a.switch_rate:
                        switch_to[i] = pick_channel(ch[i])
                        gap = switch_gap(a.quick_share)
                        quick[i] = gap <= 5.0
                    else:
                        gap = next_gap(ch[i])
                    heapq.heappush(due, (now + gap / x, i))
                # One batch per partition, all sent at once: per-customer order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["peak"] = x > 1
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[agents] {a.runtimes_per_channel * len(CHANNELS)} agent runtimes on {', '.join(CHANNELS)}, "
              f"{n_cust:,} customers in conversation, fact share {a.fact_share:.0%}, "
              f"switch rate {a.switch_rate:.0%} of turns", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            print(f"[agents] {int(stats['turns']):>7,} turns/s ({phase})  facts {int(stats['facts']):,}  "
                  f"channel switches {int(stats['switches']):,}, with memory {int(stats['with_memory']):,}, "
                  f"would ask again {int(stats['ask_again']):,} (fact from the conversation still open "
                  f"{int(stats['open_conv']):,})  memory-to-store n {r['n']:,} p50 {r['p50']:.1f}ms "
                  f"p99 {r['p99']:.1f}ms", flush=True)
            for k in ("turns", "facts", "switches", "with_memory", "ask_again", "open_conv"):
                stats[k] = 0

    await asyncio.gather(turn_loop(), report(), store_reader())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--runtimes-per-channel", type=int, default=4, help="agent runtimes on each of voice, chat, SMS")
    p.add_argument("--turns-per-s", type=float, default=5_000, help="turns per second, all channels")
    p.add_argument("--fact-share", type=float, default=0.2, help="share of turns that add a fact to memory")
    p.add_argument("--switch-rate", type=float, default=0.03, help="share of turns after which the customer "
                                                                    "carries on on another channel")
    p.add_argument("--quick-share", type=float, default=0.5, help="share of switches made 1-5 s later, while "
                                                                  "the conversation they leave is still open")
    p.add_argument("--turn-bytes", type=int, default=500, help="bytes of turn text per record")
    p.add_argument("--peak", type=float, default=1, help="turn-rate multiplier during the peak (a disruption surge)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
