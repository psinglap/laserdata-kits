"""A fleet of Forge function runtime hosts: every invocation's usage onto the lane, keyed by app.

Plays --hosts runtime hosts in one process. Apps are invoked at --invocations-per-s in total, with a skewed app
mix (a few popular apps run most invocations). Each invocation runs for a log-normal duration at MEM_GB of
memory, and the host publishes one usage record to the usage topic, keyed by app:
  * host, app, billing cycle, a per-(host, app) sequence, a usageUniqueId, the host's running Forge Functions
    duration for that app this cycle in GB-seconds (the resulting total, never only this invocation), and
    this invocation's GB-seconds (d), which only the "without dedupe" comparison uses.

At-least-once on purpose: --resend-pct of records are sent a second time, as a host that timed out waiting
for an acknowledgement would. The allowance tracker (tracker.py) must count each one once.

Prints invocations, GB-seconds served and resends per second.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (DEFAULT_URL, DUR_MU, DUR_SIGMA, MEM_GB, PARTITIONS, STREAM, USAGE, AppMix, cycle,
                    ensure_topics, msg, part)

TICK = 0.01


async def main(a):
    client = IggyClient.from_connection_string(a.url)      # one connection for this producer
    await client.connect()
    await ensure_topics(client, USAGE)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await client.producer(STREAM, USAGE, topic_partitions_count=PARTITIONS[USAGE], mode=cfg)
    hosts = [f"rt-{i:02d}" for i in range(a.hosts)]
    mix = AppMix(a.apps)
    pid = {n: part(n, PARTITIONS[USAGE]) for n in mix.names}
    seq = defaultdict(int)                  # (host, app) -> sequence
    cum = defaultdict(float)                # (cycle, host, app) -> GB-seconds this cycle
    recent = deque(maxlen=4000)             # recently sent usage, for the at-least-once resends
    stats = defaultdict(float)
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def serve():
        t0 = due = time.monotonic()
        owed = 0.0
        async with producer:
            while True:
                t = time.monotonic() - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                stats["peak"] = x > 1
                owed += a.invocations_per_s * x * TICK
                n, owed = int(owed), owed - int(owed)
                c = cycle(a.cycle_s)
                batches = defaultdict(list)
                for _ in range(n):
                    h = random.choice(hosts)
                    ap = mix.pick(random.random())
                    gbs = MEM_GB * random.lognormvariate(DUR_MU, DUR_SIGMA)
                    seq[(h, ap)] += 1
                    cum[(c, h, ap)] += gbs
                    s = seq[(h, ap)]
                    b = msg(h=h, a=ap, c=c, seq=s, id=f"{h}/{ap}/{s}", gbs=round(cum[(c, h, ap)], 5),
                            d=round(gbs, 5))
                    batches[pid[ap]].append(SendMessage(b))
                    stats["sent"] += 1
                    stats["gbs"] += gbs
                    if a.resend_pct and random.random() < a.resend_pct / 100 and recent:
                        q, old = random.choice(recent)    # a retry of an earlier record: same host, app, seq
                        batches[q].append(SendMessage(old))
                        stats["resent"] += 1
                    recent.append((pid[ap], b))
                # One batch per partition, all sent at once: an app's usage stays in order, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                due += TICK                 # from the due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "peak" if stats["peak"] else "steady"
            print(f"[runtime] {int(stats['sent']):>7,} invocations/s ({phase}) on {a.hosts} hosts  "
                  f"Forge Functions duration {stats['gbs']:,.1f} GB-s/s  resent {int(stats['resent']):,}", flush=True)
            for k in ("sent", "gbs", "resent"):
                stats[k] = 0
            old = cycle(a.cycle_s) - 2      # forget cycles that ended more than a cycle ago
            for k in [k for k in cum if k[0] <= old]:
                del cum[k]

    await asyncio.gather(serve(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--hosts", type=int, default=8, help="Forge function runtime hosts")
    p.add_argument("--apps", type=int, default=5_000, help="must match tracker.py")
    p.add_argument("--invocations-per-s", type=float, default=4_000,
                   help="must match tracker.py; 4,000/s is about 350M usage events a day")
    p.add_argument("--cycle-s", type=float, default=60, help="seconds per compressed billing cycle; must match tracker.py")
    p.add_argument("--resend-pct", type=float, default=0.5, help="share of usage records sent twice (retries)")
    p.add_argument("--peak", type=float, default=1, help="invocation-rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
