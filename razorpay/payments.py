"""Payment outcomes: one status event per payment, keyed by its cohort, on the payment-status lane.

Plays the Payment Status Job. Razorpay's 2021 downtime post describes it: about 15 workflow
events per payment, of which 3 decide the status; the job keys them by payment ID, finds the
status and emits one payment event with that status to Kafka. Here every payment's final status
(captured or failed) goes on `payment-status`, keyed by cohort (method plus instrument, the way
the Downtime API names them), with a per-cohort sequence so a reader can drop replays.

A downtime is injected every --incident-every seconds: one busy cohort's success rate falls to
--incident-sr for --incident-for seconds, as when a bank or a PSP degrades. Each payment in it
carries `deg`, the moment the incident began. The detector never reads `deg`; Checkout uses it
only to score how long the downtime went unflagged.

A surge plays a match-day peak: --surge times the payment rate for --surge-for seconds.
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

from common import DEFAULT_URL, PARTITIONS, STATUS, STREAM, cohorts, ensure_topics, msg, part

# Busy cohorts that take turns degrading, across UPI, cards and netbanking.
INCIDENTS = ["upi:psp:okhdfcbank", "card:issuer:HDFC:credit", "upi:psp:okaxis", "netbanking:bank:SBIN",
             "upi:psp:oksbi", "card:issuer:SBIN:debit"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, STATUS)
    producer = await client.producer(STREAM, STATUS, topic_partitions_count=PARTITIONS[STATUS],
                                     mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200)))
    table = cohorts()
    keys, usual = [k for k, _, _ in table], {k: sr for k, _, sr in table}
    cum = list(itertools.accumulate(w for _, w, _ in table))
    seq, stats = defaultdict(int), defaultdict(int)
    incident = {"k": None, "onset": 0, "until": 0.0, "n": 0}
    n_pay = 0
    gc.freeze()                             # keep start-up objects out of the collector's way

    def payment(now_ns, t):
        nonlocal n_pay
        n_pay += 1
        k = random.choices(keys, cum_weights=cum)[0]
        down = k == incident["k"] and t < incident["until"]
        ok = random.random() < (a.incident_sr if down else usual[k])
        seq[k] += 1
        stats["payments"] += 1
        stats["failed"] += not ok
        return k, SendMessage(msg(k=k, seq=seq[k], pay=f"pay_{n_pay:014d}", status="captured" if ok else "failed",
                                  deg=incident["onset"] if down else 0, ns=now_ns))

    async def send(batch):
        # One batch per partition, all sent at once: per-cohort order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for k, m in batch:
            groups[part(k, PARTITIONS[STATUS])].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def run():
        t0 = due = time.monotonic()
        owed, next_incident = 0.0, a.incident_at
        async with producer:
            while True:
                t = time.monotonic() - t0
                if a.incident_every and t >= next_incident:
                    k = INCIDENTS[incident["n"] % len(INCIDENTS)]
                    incident.update(k=k, onset=time.time_ns(), until=t + a.incident_for, n=incident["n"] + 1)
                    next_incident += a.incident_every
                    print(f"[payments] incident {incident['n']}: {k} success rate falls to {a.incident_sr:.0%} "
                          f"for {a.incident_for:g} s", flush=True)
                x = a.surge if a.surge_at <= t < a.surge_at + a.surge_for else 1
                owed += a.payments_per_s * x / 100
                now = time.time_ns()
                made = [payment(now, t) for _ in range(int(owed))]
                owed -= int(owed)
                if made:
                    await send(made)
                stats["surge"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "surge" if stats["surge"] else "steady"
            sr = 100 * (1 - stats["failed"] / stats["payments"]) if stats["payments"] else 0
            print(f"[payments] {stats['payments']:>7,} payments/s ({phase})  success rate {sr:.1f}%", flush=True)
            stats["payments"] = stats["failed"] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--payments-per-s", type=float, default=2_000)
    p.add_argument("--surge", type=float, default=5, help="payment-rate multiplier during the surge")
    p.add_argument("--surge-at", type=float, default=0, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=60, help="seconds")
    p.add_argument("--incident-at", type=float, default=15, help="first downtime, seconds after start")
    p.add_argument("--incident-every", type=float, default=30, help="seconds between downtimes; 0 for none")
    p.add_argument("--incident-for", type=float, default=12, help="seconds each downtime lasts")
    p.add_argument("--incident-sr", type=float, default=0.10, help="success rate of the degraded cohort")
    asyncio.run(main(p.parse_args()))
