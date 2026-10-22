"""The status webhook receiver: every Sent, Delivered and Read webhook onto its own Apache Iggy topic.

Plays the point where Netcore's webhook endpoint has accepted a provider's status callback.
Outbound messages leave at --send-rate a second across --campaigns campaigns (a few large
broadcasts and a long tail). Each message comes back as a Sent webhook at once, a Delivered
webhook a few seconds later for most messages, and a Read webhook later still for some.
Providers retry: --retry of webhooks arrive a second time with the same message ID.

--surge-rate adds extra Read webhooks for --surge-for seconds, from messages sent earlier on
the largest campaign: the moment a big broadcast is opened. The surge goes to status-read
only; watch reporter.py --status delivered while it runs.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, STATUSES, STREAM, campaign_name, connect, ensure_topic, msg, part, weights


async def main(a):
    admin = await connect(a.url)
    for topic, parts in STATUSES.values():
        await ensure_topic(admin, STREAM, topic, parts)
    print(f"[webhooks] status topics ready in '{STREAM}': "
          + ", ".join(f"{t} ({p} partitions)" for t, p in STATUSES.values()), flush=True)
    writers = [await connect(a.url) for _ in range(a.writers)]
    w = weights(a.campaigns)
    camps = [campaign_name(i) for i in range(a.campaigns)]
    tick_s = a.tick_ms / 1000
    due = defaultdict(list)                    # tick number -> [(status, message ID, campaign, retry)]
    stats = defaultdict(int)
    owed, surge_owed, n_msg = 0.0, 0.0, 0
    gc.freeze()                                # keep the collector from pausing the receiver

    def ticks(lo, hi):
        return max(1, int(random.uniform(lo, hi) / tick_s))

    async def send(c, batch):
        await asyncio.gather(*(c.send_messages(STREAM, STATUSES[s][0], Partitioning.partition_id(pid), ms)
                               for (s, pid), ms in batch.items()))

    async def run():
        nonlocal owed, surge_owed, n_msg
        t0 = tick_t = time.monotonic()
        k = 0
        while True:
            t = time.monotonic() - t0
            now = due.pop(k, [])
            owed += a.send_rate * tick_s
            new = int(owed)
            owed -= new
            for _ in range(new):               # new outbound messages: Sent now, the rest scheduled
                n_msg += 1
                m = f"m{n_msg:010d}"
                c = random.choices(camps, w)[0]
                now.append(("sent", m, c, 0))
                if random.random() < a.delivered:
                    due[k + ticks(a.delivered_after[0], a.delivered_after[1])].append(("delivered", m, c, 0))
                    if random.random() < a.read:
                        due[k + ticks(a.read_after[0], a.read_after[1])].append(("read", m, c, 0))
            if a.surge_at <= t < a.surge_at + a.surge_for:
                surge_owed += a.surge_rate * tick_s
                extra = int(surge_owed)
                surge_owed -= extra
                for _ in range(extra):         # the big broadcast being opened: Read webhooks only
                    now.append(("read", f"s{random.randrange(10**10):010d}", camps[0], 0))
                stats["surge"] += extra
            batches = [defaultdict(list) for _ in writers]
            for s, m, c, r in now:
                if not r and random.random() < a.retry:
                    due[k + ticks(1, 5)].append((s, m, c, 1))       # the provider will send this one again
                body = SendMessage(msg(m=m, c=c, s=s, r=r) if r else msg(m=m, c=c, s=s))
                pid = part(m, STATUSES[s][1])
                batches[pid % len(writers)][s, pid].append(body)
                stats[s] += 1
            # One batch per status topic partition per tick, each writer on its own connection.
            await asyncio.gather(*(send(c, b) for c, b in zip(writers, batches) if b))
            k += 1
            tick_t = max(tick_t + tick_s, time.monotonic() - 0.1)   # due time, never banking more than 100 ms
            await asyncio.sleep(max(0.0, tick_t - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            total = stats["sent"] + stats["delivered"] + stats["read"]
            extra = f"  Read surge on {camps[0]}: +{stats['surge']:,}/s" if stats["surge"] else ""
            print(f"[webhooks] {total:>7,} status webhooks/s  sent {stats['sent']:,}  delivered {stats['delivered']:,}  "
                  f"read {stats['read']:,}{extra}", flush=True)
            stats.clear()

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--send-rate", type=float, default=4_000, help="outbound messages a second (the delivery engine's rate)")
    p.add_argument("--campaigns", type=int, default=200, help="campaigns sending at once")
    p.add_argument("--delivered", type=float, default=0.95, help="share of messages that come back Delivered")
    p.add_argument("--delivered-after", type=float, nargs=2, default=[0.3, 3.0], help="seconds after send, min max")
    p.add_argument("--read", type=float, default=0.4, help="share of delivered messages that come back Read")
    p.add_argument("--read-after", type=float, nargs=2, default=[3.0, 60.0], help="seconds after delivered, min max")
    p.add_argument("--retry", type=float, default=0.02, help="share of webhooks the provider sends twice")
    p.add_argument("--writers", type=int, default=2, help="connections sending in parallel")
    p.add_argument("--tick-ms", type=float, default=20)
    p.add_argument("--surge-rate", type=float, default=20_000, help="extra Read webhooks a second during the surge")
    p.add_argument("--surge-at", type=float, default=60, help="seconds before the Read surge starts")
    p.add_argument("--surge-for", type=float, default=0, help="Read surge length in seconds (0 = none)")
    asyncio.run(main(p.parse_args()))
