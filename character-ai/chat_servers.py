"""A fleet of chat servers: every turn onto the lane, and every safety verdict applied before the conversation's next turn.

Plays --servers chat servers in one process. Conversations take turns at illustrative gaps (typed replies
3-30 s apart, swipes and regenerations 0.3-3 s apart, see common.next_gap), and each turn lands on any
chat server, as a load balancer would send it. For every turn the server publishes to the turns topic,
keyed by conversation:
  * conversation, chat server, the conversation's turn sequence, and the turn text (the user's message and
    the character's reply; a share of turns carry content a classifier will flag).
Each chat server also reads safety-verdicts on its own connection and consumer group. A verdict names the
conversation and the turn it covers: BLOCK takes that content out of the conversation, so the next prompt
must not carry it; LIFELINE shows the Lifeline pop-up on the next reply.

The business number: of the turns that came right after a flagged turn, how many were built before that
chat server had the verdict, so the prompt still carried blocked content or the pop-up was missed. Prints
it every second, with verdict-to-chat-server p50/p99: the second hop of the loop.
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

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (BLOCK, DEFAULT_URL, LIFELINE, PARTITIONS, STREAM, TURNS, VERDICTS, Window, conversation,
                    ensure_topics, load, mean_gap, msg, next_gap, part)

# Stands in for a user's message and the character's reply; the real lane carries your turn text.
FILLER = ("hey, can you tell me more about the castle? *the knight lowers her sword and smiles* Of course. "
          "It was built on the cliffs long before the war, and the old library is still there. ") * 20


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), TURNS, VERDICTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, TURNS, topic_partitions_count=PARTITIONS[TURNS], mode=cfg)
    start_ns = time.time_ns()
    servers = [f"chat-{i:02d}" for i in range(a.servers)]
    n_conv = max(1, int(a.turns_per_s * mean_gap(a.swipe_share)))
    convs = [conversation(i) for i in range(n_conv)]
    pid = {c: part(c, PARTITIONS[TURNS]) for c in convs}
    now = time.monotonic()
    due = [(now + random.uniform(0, next_gap(a.swipe_share)), i) for i in range(n_conv)]
    heapq.heapify(due)
    view = {s: {} for s in servers}         # chat server -> {conversation: latest flagged turn it has a verdict for}
    seq = defaultdict(int)                  # conversation -> turn sequence
    flagged = {}                            # conversation -> (flagged turn seq, ns): waiting for its next turn
    lat, stats = Window(), defaultdict(float)

    def reader(s):
        async def on_verdict(m):
            d = load(m.payload())
            if d["t"] <= view[s].get(d["c"], 0):
                return                      # older or repeated verdict: the newest wins
            view[s][d["c"]] = d["t"]
            if d["ns"] > start_ns:
                lat.add((time.time_ns() - d["ns"]) / 1e6)   # live verdicts only, not ones replayed at start-up
        return on_verdict

    readers = []
    for s in servers:
        readers.append((await (await own_client()).consumer_group(
            f"{s}-{os.getpid()}", STREAM, VERDICTS, polling_strategy=PollingStrategy.First(), batch_length=1000,
            auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500)), reader(s)))
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
                    c, s = convs[i], random.choice(servers)
                    if c in flagged:        # the turn right after a flagged turn: is the verdict here yet?
                        t, f_ns = flagged.pop(c)
                        stats["after_flag"] += 1
                        if view[s].get(c, 0) < t:
                            stats["early"] += 1     # prompt built before the verdict reached this server
                        if (time.time_ns() - f_ns) / 1e6 < a.compare_ms:
                            stats["early_at_compare"] += 1
                    seq[c] += 1
                    r = random.random()
                    tag = ""
                    if r < a.flag_rate:
                        tag = f"[{LIFELINE if r < a.flag_rate * 0.1 else BLOCK}] "
                        flagged[c] = (seq[c], time.time_ns())
                        stats["flagged"] += 1
                    off = random.randrange(200)
                    batches[pid[c]].append(SendMessage(msg(c=c, s=s, seq=seq[c],
                                                           text=tag + FILLER[off:off + a.turn_bytes])))
                    stats["turns"] += 1
                    heapq.heappush(due, (now + next_gap(a.swipe_share) / x, i))
                # One batch per partition, all sent at once: per-conversation order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["peak"] = x > 1
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[chat] {a.servers} chat servers, {n_conv:,} active conversations, flag rate {a.flag_rate:.1%}", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            print(f"[chat] {int(stats['turns']):>7,} turns/s ({phase})  flagged {int(stats['flagged']):,}  "
                  f"next turns after a flag {int(stats['after_flag']):,}, served before the verdict "
                  f"{int(stats['early']):,} (at {a.compare_ms:g} ms it would be {int(stats['early_at_compare']):,})  "
                  f"verdict-to-chat-servers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("turns", "flagged", "after_flag", "early", "early_at_compare"):
                stats[k] = 0

    await asyncio.gather(turn_loop(), report(), *(c.consume_messages(cb, asyncio.Event()) for c, cb in readers))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--servers", type=int, default=8, help="chat servers, each with its own verdict reader")
    p.add_argument("--turns-per-s", type=float, default=10_000, help="chat turns per second, all servers")
    p.add_argument("--flag-rate", type=float, default=0.01, help="share of turns a classifier flags (illustrative)")
    p.add_argument("--swipe-share", type=float, default=0.3, help="share of turns that are swipes or regenerations")
    p.add_argument("--turn-bytes", type=int, default=600, help="bytes of turn text per record")
    p.add_argument("--compare-ms", type=float, default=900, help="also count next turns that would beat a lane this slow")
    p.add_argument("--peak", type=float, default=1, help="turn-rate multiplier during the peak (an evening surge)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
