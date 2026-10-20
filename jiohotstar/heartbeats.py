"""Playback heartbeats: the minimized heartbeat of every live session, keyed by cohort and CDN.

Plays the output of JioHotstar's heartbeat splitter. Its Oct 2026 post describes a 7.5 KB heartbeat
with 300+ fields from every active playback session, and a minimized ~1.1 KB HeartbeatEvent of the
80-90 fields the subset consumers need. Here each heartbeat carries its session, its state (playing,
buffering or failed), RTT, and padding up to --size bytes, on `heartbeats-lite`, keyed by
cohort|cdn (network plus state, as the traffic-orchestration post's ASN-State cohorts), with a
per-key sequence so a reader can drop replays.

An incident is injected every --incident-every seconds: one busy cohort on one CDN starts failing
and rebuffering for --incident-for seconds, as when an edge degrades in one city. Each heartbeat in
it carries `deg`, the moment the incident began. The health check never reads `deg`; routing uses it
only to score how long the incident went unflagged.

The death overs: --surge times the heartbeat rate for --surge-for seconds, as viewers pile in.
"""
import argparse
import asyncio
import gc
import itertools
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, HEARTBEATS, PARTITIONS, STREAM, ensure_topics, msg, pairs, part


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, HEARTBEATS)
    producer = await client.producer(STREAM, HEARTBEATS, topic_partitions_count=PARTITIONS[HEARTBEATS],
                                     mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200)))
    table = pairs()
    keys, cum = [k for k, _ in table], list(itertools.accumulate(w for _, w in table))
    busiest = [k for k, _ in sorted(table, key=lambda kw: -kw[1])][:6]   # busy pairs take turns degrading
    pad = "x" * max(0, a.size - 190)        # stands in for the user, device, content and QoS fields kept
    seq, stats = defaultdict(int), defaultdict(int)
    incident = {"k": None, "onset": 0, "until": 0.0, "n": 0}
    n_hb = 0
    gc.freeze()                             # keep start-up objects out of the collector's way

    def heartbeat(now_ns, t):
        nonlocal n_hb
        n_hb += 1
        k = random.choices(keys, cum_weights=cum)[0]
        bad = k == incident["k"] and t < incident["until"]
        r = random.random()
        if bad:
            st = "failed" if r < a.incident_pfr else "buffering" if r < a.incident_pfr + 0.30 else "playing"
            rtt = random.randint(350, 900)
        else:
            st = "failed" if r < 0.01 else "buffering" if r < 0.04 else "playing"
            rtt = random.randint(25, 120)
        seq[k] += 1
        stats["heartbeats"] += 1
        stats["failed"] += st == "failed"
        return k, SendMessage(msg(k=k, seq=seq[k], sess=f"s{n_hb % 5_000_000:07d}", st=st, rtt=rtt,
                                  deg=incident["onset"] if bad else 0, ctx=pad, ns=now_ns))

    async def send(batch):
        # One batch per partition, all sent at once: per-key order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for k, m in batch:
            groups[part(k, PARTITIONS[HEARTBEATS])].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def run():
        t0 = due = time.monotonic()
        owed, next_incident = 0.0, a.incident_at
        async with producer:
            while True:
                t = time.monotonic() - t0
                if a.incident_every and t >= next_incident:
                    k = busiest[incident["n"] % len(busiest)]
                    incident.update(k=k, onset=time.time_ns(), until=t + a.incident_for, n=incident["n"] + 1)
                    next_incident += a.incident_every
                    print(f"[heartbeats] incident {incident['n']}: {k} fails {a.incident_pfr:.0%} of heartbeats "
                          f"for {a.incident_for:g} s", flush=True)
                x = a.surge if a.surge_at <= t < a.surge_at + a.surge_for else 1
                owed += a.heartbeats_per_s * x / 100
                now = time.time_ns()
                made = [heartbeat(now, t) for _ in range(int(owed))]
                owed -= int(owed)
                if made:
                    await send(made)
                stats["surge"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "death overs" if stats["surge"] else "steady"
            pfr = 100 * stats["failed"] / stats["heartbeats"] if stats["heartbeats"] else 0
            print(f"[heartbeats] {stats['heartbeats']:>7,} heartbeats/s ({phase})  failed {pfr:.2f}%", flush=True)
            stats["heartbeats"] = stats["failed"] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--heartbeats-per-s", type=float, default=10_000)
    p.add_argument("--size", type=int, default=1100, help="bytes per heartbeat, like the minimized HeartbeatEvent")
    p.add_argument("--surge", type=float, default=3, help="heartbeat-rate multiplier in the death overs")
    p.add_argument("--surge-at", type=float, default=0, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=60, help="seconds")
    p.add_argument("--incident-at", type=float, default=15, help="first incident, seconds after start")
    p.add_argument("--incident-every", type=float, default=30, help="seconds between incidents; 0 for none")
    p.add_argument("--incident-for", type=float, default=12, help="seconds each incident lasts")
    p.add_argument("--incident-pfr", type=float, default=0.25, help="share of heartbeats that report a failure")
    asyncio.run(main(p.parse_args()))
