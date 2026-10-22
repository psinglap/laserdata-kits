"""vProbes: one Streaming Data Record per control-plane procedure, keyed by the network-function instance.

Plays the vProbes that Jio's Cloud Native 5G Core whitepaper describes as embedded in the network
functions, collecting Streaming Data Records (SDRs) from 4G/5G network nodes. Each record carries the NF
instance (for example amf-mu-02), the procedure (registration, PDU session establishment, N4 session...),
its result and cause, how long it took, a procedure ID, and padding up to --size bytes for the rest of the
record. It goes on `vprobe-sdr`, keyed by NF instance, with a per-instance sequence so a reader can drop
replays. No subscriber identity is in the record.

An incident is injected every --incident-every seconds: one busy NF instance starts failing
--incident-fail of its procedures, slowly, for --incident-for seconds, as when one CNF degrades. Each record
in it carries `deg`, the moment the incident began. The anomaly check never reads `deg`; the workflow engine
uses it only to score how long the incident went unflagged.

A signalling storm: --storm times the record rate for --storm-for seconds, as when a large site comes back
and its devices re-register at once.
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

from common import CAUSE, DEFAULT_URL, PARTITIONS, PROCEDURES, SDRS, STREAM, ensure_topics, msg, nfs, part


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, SDRS)
    producer = await client.producer(STREAM, SDRS, topic_partitions_count=PARTITIONS[SDRS],
                                     mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200)))
    table = nfs()
    keys, cum = [k for k, _ in table], list(itertools.accumulate(w for _, w in table))
    busiest = [k for k, _ in sorted(table, key=lambda kw: -kw[1])][:6]   # busy instances take turns degrading
    pad = "x" * max(0, a.size - 200)        # stands in for the rest of the record: node, interface, timers, counters
    seq, stats = defaultdict(int), defaultdict(int)
    incident = {"k": None, "onset": 0, "until": 0.0, "n": 0}
    n_rec = 0
    gc.freeze()                             # keep start-up objects out of the collector's way

    def record(now_ns, t):
        nonlocal n_rec
        n_rec += 1
        k = random.choices(keys, cum_weights=cum)[0]
        nf_type = k.split("-", 1)[0]
        bad = k == incident["k"] and t < incident["until"]
        if bad:
            fail = random.random() < a.incident_fail
            dur = random.randint(250, 900)
        else:
            fail = random.random() < 0.002
            dur = random.randint(8, 60)
        seq[k] += 1
        stats["records"] += 1
        stats["failed"] += fail
        return k, SendMessage(msg(k=k, seq=seq[k], pid=f"p{n_rec % 10_000_000:08d}",
                                  proc=random.choice(PROCEDURES[nf_type]), res="fail" if fail else "ok",
                                  cause=CAUSE[nf_type] if fail else "", dur=dur,
                                  deg=incident["onset"] if bad else 0, rec=pad, ns=now_ns))

    async def send(batch):
        # One batch per partition, all sent at once: per-key order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for k, m in batch:
            groups[part(k, PARTITIONS[SDRS])].append(m)
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
                    print(f"[vprobes] incident {incident['n']}: {k} fails {a.incident_fail:.0%} of procedures "
                          f"for {a.incident_for:g} s", flush=True)
                x = a.storm if a.storm_at <= t < a.storm_at + a.storm_for else 1
                owed += a.records_per_s * x / 100
                now = time.time_ns()
                made = [record(now, t) for _ in range(int(owed))]
                owed -= int(owed)
                if made:
                    await send(made)
                stats["storm"] = x > 1
                due += 0.01                 # from the due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "signalling storm" if stats["storm"] else "steady"
            fr = 100 * stats["failed"] / stats["records"] if stats["records"] else 0
            print(f"[vprobes] {stats['records']:>7,} SDRs/s ({phase})  failed {fr:.2f}%", flush=True)
            stats["records"] = stats["failed"] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--records-per-s", type=float, default=20_000)
    p.add_argument("--size", type=int, default=600, help="bytes per record (illustrative)")
    p.add_argument("--storm", type=float, default=3, help="record-rate multiplier in the signalling storm")
    p.add_argument("--storm-at", type=float, default=30, help="seconds after start")
    p.add_argument("--storm-for", type=float, default=60, help="seconds")
    p.add_argument("--incident-at", type=float, default=15, help="first incident, seconds after start")
    p.add_argument("--incident-every", type=float, default=30, help="seconds between incidents; 0 for none")
    p.add_argument("--incident-for", type=float, default=12, help="seconds each incident lasts")
    p.add_argument("--incident-fail", type=float, default=0.25, help="share of the instance's procedures that fail")
    asyncio.run(main(p.parse_args()))
