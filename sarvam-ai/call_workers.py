"""A fleet of voice-agent workers: every conversation start and end onto the lane, every headroom change applied
before the next conversation starts.

Plays --workers workers in one process. Conversations arrive for --orgs organizations: campaign calls and instant
outbound calls, and inbound calls. Before a conversation starts, the worker it lands on checks its own view of
that organization's headroom:
  * outbound (campaign or instant) starts only while the organization is OPEN; otherwise the attempt is held
    back, and the dialer tries again later;
  * inbound starts unless the organization is FULL, into the capacity kept for inbound; otherwise it is
    turned away.
A conversation holds its slot for its talk time (exponential, mean --talk-s), as Sarvam's docs describe: from the
moment it starts until it ends, never while dialing or ringing.

For every start and end the worker publishes to the slot-events topic, keyed by organization:
  * worker, organization, a per-(worker, organization) sequence and the worker's live conversations for that
    organization (the resulting count, never only +1 or -1), plus the source and the event;
and once a second a beat, so the ledger can release the slots of a worker that stops.
Each worker reads org-headroom on its own connection and consumer group, and applies it to its own view; a
worker never sees another's view, so a change reaches every worker through the log.

Prints conversations started per second, attempts held back and inbound calls turned away, decision-to-workers
p50/p99 (the second hop of the loop), and what a view one second behind would have done with the same
attempts, judged against the same view at the same moment.
"""
import argparse
import asyncio
import bisect
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, FULL, HEADROOM, INBOUND_SHARE, INSTANT_SHARE, OPEN, PARTITIONS, SLOTS, STREAM,
                    Window, ensure_topics, load, msg, org, part, weight)


def allowed(st: str, src: str) -> bool:
    return st == OPEN if src != "inbound" else st != FULL


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SLOTS, HEADROOM)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, SLOTS, topic_partitions_count=PARTITIONS[SLOTS], mode=cfg)
    start_ns = time.time_ns()
    run = f"{os.getpid() % 10000:04d}"
    workers = [f"wkr-{i:02d}-{run}" for i in range(a.workers)]
    orgs = [org(i) for i in range(a.orgs)]
    weights = [weight(o) for o in orgs]
    pid = {o: part(o, PARTITIONS[SLOTS]) for o in orgs}
    view = {w: {} for w in workers}         # worker -> {org: (state, decision ns)}
    hist = {w: defaultdict(list) for w in workers}   # worker -> org -> [(arrival ns, state)], for the 1 s-behind view
    seq, live = defaultdict(int), defaultdict(int)
    ends = []                               # heap of (end time, worker, org)
    lat, stats = Window(), defaultdict(float)

    def reader(w):
        async def on_decision(m):
            d = load(m.payload())
            v = view[w].get(d["o"])
            if v and d["ns"] <= v[1]:
                return                      # older or repeated decision: the newest wins
            view[w][d["o"]] = (d["state"], d["ns"])
            now = time.time_ns()
            h = hist[w][d["o"]]
            h.append((now, d["state"]))
            while len(h) > 1 and h[1][0] < now - 2_000_000_000:
                del h[0]                    # keep the last state older than the 1 s-behind point, drop the rest
            if d["ns"] > start_ns:
                lat.add((now - d["ns"]) / 1e6)   # live decisions only, not ones replayed at start-up
        return on_decision

    readers = []
    for w in workers:
        readers.append((await (await own_client()).consumer_group(
            f"{w}-hd", STREAM, HEADROOM, polling_strategy=PollingStrategy.First(), batch_length=1000,
            auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500)), reader(w)))
    gc.freeze()                             # keep start-up objects out of the collector's way

    def behind(w, o, now):
        # The state this worker would hold if every decision reached it one second later.
        h = hist[w].get(o)
        if not h:
            return OPEN
        i = bisect.bisect_right(h, (now - 1_000_000_000, "~")) - 1
        return h[i][1] if i >= 0 else OPEN

    def slot(batches, w, o, ev, src):
        seq[(w, o)] += 1
        batches[pid[o]].append(SendMessage(msg(w=w, o=o, seq=seq[(w, o)], live=live[(w, o)], ev=ev, src=src)))

    async def call_loop():
        t0 = due = time.monotonic()
        owed, next_beat = 0.0, 0.0
        async with producer:
            while True:
                t = time.monotonic() - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.conv_per_s * x / 100
                batches = defaultdict(list)
                now_m, now_ns = time.monotonic(), time.time_ns()
                while ends and ends[0][0] <= now_m:          # conversations that ended free their slots first
                    _, w, o = heapq.heappop(ends)
                    live[(w, o)] -= 1
                    slot(batches, w, o, "end", "")
                for o in random.choices(orgs, weights, k=int(owed)):
                    r = random.random()
                    src = "inbound" if r < INBOUND_SHARE else "instant" if r < INBOUND_SHARE + INSTANT_SHARE else "campaign"
                    w = random.choice(workers)
                    v = view[w].get(o)
                    now_ok = allowed(v[0] if v else OPEN, src)
                    late_ok = allowed(behind(w, o, now_ns), src)  # same attempt, same moment, a 1 s-old view
                    if src == "inbound":
                        stats["late_turned_away"] += now_ok and not late_ok
                    else:
                        stats["late_over"] += late_ok and not now_ok
                        stats["late_idle"] += now_ok and not late_ok
                    if not now_ok:
                        stats["turned_away" if src == "inbound" else "held"] += 1
                        continue
                    live[(w, o)] += 1
                    heapq.heappush(ends, (now_m + min(random.expovariate(1 / a.talk_s), 8 * a.talk_s), w, o))
                    slot(batches, w, o, "start", src)
                    stats["in" if src == "inbound" else "out"] += 1
                owed -= int(owed)
                if t >= next_beat:                  # one beat per worker a second: the ledger's lease
                    next_beat += 1
                    for w in workers:
                        batches[part(w, PARTITIONS[SLOTS])].append(SendMessage(msg(w=w, beat=1)))
                # One batch per partition, all sent at once: per-org order is kept, about one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            print(f"[workers] {int(stats['out'] + stats['in']):>5,} conversations started/s ({phase}; outbound "
                  f"{int(stats['out']):,} inbound {int(stats['in']):,})  live {len(ends):,}  held back at the "
                  f"ceiling {int(stats['held']):,}  inbound turned away {int(stats['turned_away']):,}  "
                  f"decision-to-workers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            print(f"[1s-behind] with every decision 1 s late: outbound started past the share "
                  f"{int(stats['late_over']):,}  inbound turned away {int(stats['late_turned_away']):,}  "
                  f"outbound held with room free {int(stats['late_idle']):,}", flush=True)
            for k in ("out", "in", "held", "turned_away", "late_over", "late_turned_away", "late_idle"):
                stats[k] = 0

    await asyncio.gather(call_loop(), report(), *(c.consume_messages(cb, asyncio.Event()) for c, cb in readers))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--workers", type=int, default=12, help="voice-agent workers, each with its own headroom reader")
    p.add_argument("--orgs", type=int, default=300, help="must match ledger.py")
    p.add_argument("--conv-per-s", type=float, default=60, help="conversations offered per second, all orgs; must match ledger.py")
    p.add_argument("--talk-s", type=float, default=90, help="average talk time in seconds; must match ledger.py")
    p.add_argument("--peak", type=float, default=1, help="call-rate multiplier during the peak (a campaign launch)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
