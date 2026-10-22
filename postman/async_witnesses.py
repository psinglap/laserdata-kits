"""async_witnesses, the receive side: every Insights Agent's upload batch onto the lane, keyed by service.

Plays --agents Insights Agents and the service that receives their uploads, in one process. Each agent
watches one service (a skewed mix: a few busy services, a long tail) and, like the real agent, flushes one
upload batch every --flush-s seconds (5 s by default), with the agents' flushes spread over that interval.
A batch is published to the witness-batches topic as one message, keyed by service:
  * client_id, service, the agent's batch number (b, counting up from 1), the moment the agent dispatched it,
    and its witnesses, each as [endpoint, status, latency ms] (a compact stand-in for the witness proto).

Two things go wrong on purpose:
  * --lost-pct of uploads never arrive (a 429 or a timeout at the agent). The agent's batch number still
    moves on, so witness_assembler can name the missing batch from the next one.
  * --resend-pct of batches are published twice, as a publish retried after a timeout would be.
witness_assembler (assembler.py) must name every lost batch and count every resent one once.

With --duration it stops on its own, sends each agent's last batch (never lost, as an agent flushes on
shutdown), and writes the totals to --totals for the check in ci.sh. Prints batches, witnesses, publish
p99 (what the receiving service waits for) and lost uploads every second.
"""
import argparse
import asyncio
import gc
import json
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (DEFAULT_URL, FLUSH_S, PARTITIONS, STREAM, WITNESSES, Mix, Window, endpoints, ensure_topics,
                    msg, part, service)

TICK = 0.01
STATUS = [200] * 80 + [201] * 8 + [204] * 3 + [400, 401, 403, 404, 404, 409, 429] + [500, 503]
EP_WEIGHTS = [30, 18, 10, 6, 2, 5, 8, 6, 9, 2, 3, 1]       # GET-heavy, health checks included (illustrative)


async def main(a):
    client = IggyClient.from_connection_string(a.url)      # one connection for this producer
    await client.connect()
    await ensure_topics(client, WITNESSES)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await client.producer(STREAM, WITNESSES, topic_partitions_count=PARTITIONS[WITNESSES], mode=cfg)

    mix = Mix([service(i) for i in range(a.services)])
    agents = []                             # (client_id, service, witnesses per second)
    for i in range(a.agents):
        agents.append([f"cli_{i:05d}", mix.pick(random.random()), 0.0])
    per_svc = defaultdict(int)
    for _, s, _ in agents:
        per_svc[s] += 1
    covered = sum(mix.share[s] for s in per_svc)
    for ag in agents:                       # each service's traffic shared by the agents watching it
        ag[2] = a.witnesses_per_s * mix.share[ag[1]] / covered / per_svc[ag[1]]
    pid = {s: part(s, PARTITIONS[WITNESSES]) for s in per_svc}
    eps = list(range(len(endpoints(service(0)))))
    pool = [[random.choices(eps, EP_WEIGHTS)[0], random.choice(STATUS), round(random.lognormvariate(3.3, 0.8), 1)]
            for _ in range(20_000)]       # witnesses to draw from: endpoint index, HTTP status, latency ms

    bseq = defaultdict(int)                 # client -> last batch number
    recent = deque(maxlen=2000)             # recently published batches, for the retried publishes
    tot = defaultdict(int)                  # since start, for --totals
    stats = defaultdict(float)
    pub = Window()
    gc.freeze()                             # keep start-up objects out of the collector's way

    def batch(ag, t, last=False):
        c, s, rate = ag
        x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
        n = max(1, int(random.gauss(rate * a.flush_s * x, (rate * a.flush_s * x) ** 0.5)))
        bseq[c] += 1
        if not last and random.random() < a.lost_pct / 100:
            tot["lost_batches"] += 1        # the upload failed at the agent; the batch number has moved on
            tot["lost_witnesses"] += n
            stats["lost"] += 1
            return None
        w = random.choices(pool, k=n)
        body = dict(c=c, s=s, b=bseq[c], up=time.time_ns(), w=w)
        if last:
            body["last"] = 1
        tot["batches"] += 1
        tot["witnesses"] += n
        stats["batches"] += 1
        stats["witnesses"] += n
        return body

    async def publish(bodies):
        groups = defaultdict(list)
        for d in bodies:
            b = msg(**d)                    # ns: the moment it goes onto the lane
            groups[pid[d["s"]]].append(SendMessage(b))
            if a.resend_pct and random.random() < a.resend_pct / 100 and recent:
                q, old = random.choice(recent)  # a retried publish of an earlier batch: same client, same number
                groups[q].append(SendMessage(old))
                stats["resent"] += 1
                tot["resent"] += 1
            recent.append((pid[d["s"]], b))
        if not groups:
            return
        t0 = time.perf_counter()
        # One send per partition, all at once: a service's batches stay in order, one round trip per tick.
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                               for q, ms in groups.items()))
        pub.add((time.perf_counter() - t0) * 1000)

    async def serve():
        order = list(range(len(agents)))
        phase = [i * a.flush_s / len(agents) for i in order]   # flushes spread over the interval
        t0 = due = time.monotonic()
        nxt, rnd = 0, 0
        while not a.duration or time.monotonic() - t0 < a.duration:
            t = time.monotonic() - t0
            stats["peak"] = a.peak_at <= t < a.peak_at + a.peak_for and a.peak > 1
            bodies = []
            while t0 + rnd * a.flush_s + phase[nxt] <= time.monotonic():
                d = batch(agents[nxt], t)
                if d:
                    bodies.append(d)
                nxt += 1
                if nxt == len(agents):
                    nxt, rnd = 0, rnd + 1
            await publish(bodies)
            due += TICK                     # from the due time, so a slow tick doesn't drift the schedule
            await asyncio.sleep(max(0.0, due - time.monotonic()))
        await publish([batch(ag, time.monotonic() - t0, last=True) for ag in agents])   # flush on shutdown

    async def report():
        while True:
            await asyncio.sleep(1)
            r = pub.flush()
            phase = "peak" if stats["peak"] else "steady"
            print(f"[async_witnesses] {int(stats['batches']):>5,} batches/s from {len(agents):,} agents  "
                  f"{int(stats['witnesses']):>7,} witnesses/s ({phase})  publish p50 {r['p50']:5.1f}ms  "
                  f"p99 {r['p99']:5.1f}ms  lost uploads {int(stats['lost'])}  resent {int(stats['resent'])}",
                  flush=True)
            for k in ("batches", "witnesses", "lost", "resent"):
                stats[k] = 0

    reporter = asyncio.create_task(report())
    async with producer:
        await serve()
    reporter.cancel()
    tot["agents"] = len(agents)
    with open(a.totals, "w") as f:
        json.dump(tot, f)
    print(f"[async_witnesses] done: published {tot['batches']:,} batches with {tot['witnesses']:,} witnesses, "
          f"lost {tot['lost_batches']:,} uploads ({tot['lost_witnesses']:,} witnesses), resent {tot['resent']:,} "
          f"-> {a.totals}", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--agents", type=int, default=300, help="Insights Agents, one per monitored pod")
    p.add_argument("--services", type=int, default=60)
    p.add_argument("--witnesses-per-s", type=float, default=5_000,
                   help="5,000/s is about 3 billion witnesses a week")
    p.add_argument("--flush-s", type=float, default=FLUSH_S, help="seconds between an agent's upload batches")
    p.add_argument("--lost-pct", type=float, default=0.5, help="share of uploads that never arrive")
    p.add_argument("--resend-pct", type=float, default=0.5, help="share of batches published twice")
    p.add_argument("--peak", type=float, default=1, help="traffic multiplier during the busy hour")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    p.add_argument("--duration", type=float, default=0, help="stop after this many seconds (0 = run until stopped)")
    p.add_argument("--totals", default="async_witnesses.totals.json")
    asyncio.run(main(p.parse_args()))
