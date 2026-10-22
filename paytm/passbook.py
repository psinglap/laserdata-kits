"""The merchant passbook, fed by the late confirmations topic.

When a bank comes back after an outage, payments its customers made minutes ago are confirmed in a burst. The
confirmation step puts every confirmation older than common.FRESH_S (since the customer paid) on
late-confirmations instead of the live lane, so the burst never queues ahead of the payment being made at the
counter now, and the Soundbox never announces a payment from ten minutes ago as if it just happened. This reader
files those confirmations into each merchant's passbook (the business app's payment history and today's
collections) at its own pace, in sequence order per device, dropping callbacks delivered twice.

Reads every partition directly from its own position, storing it on the broker at most once a second, so a
restarted reader resumes where it stopped. Prints, every second, what it filed, how far behind the newest late
confirmation it is, and running totals that ci.sh checks against what payments.py sent.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, LATE, PARTITIONS, STREAM, ensure_topics, load


async def main(a):
    c = IggyClient.from_connection_string(a.url)
    await c.connect()
    await ensure_topics(c, LATE)
    seq = {}                                # Soundbox -> last sequence filed
    stats, total = defaultdict(float), defaultdict(int)
    gc.freeze()

    def on_event(m, now):
        d = load(m.payload())
        stats["lag"] = max(stats["lag"], (now - d["ns"]) / 1e9)
        stats["oldest"] = max(stats["oldest"], (now - d["vt"]) / 60e9)
        if d["seq"] <= seq.get(d["d"], 0):
            stats["repeats"] += 1
            total["repeats"] += 1
            return
        seq[d["d"]] = d["seq"]
        stats["filed"] += 1
        stats["paise"] += d["amt"]
        total["filed"] += 1

    async def read():
        who = {p: Consumer.Single(f"{a.name}-p{p}") for p in range(PARTITIONS[LATE])}
        nxt, committed = {}, {}
        while True:
            more = False
            for p in who:
                commit = time.monotonic() - committed.get(p, 0) >= 1
                ms = await c.poll_messages(STREAM, LATE, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=commit,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.Next())
                if ms:
                    if commit:
                        committed[p] = time.monotonic()
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_event(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            print(f"[{a.name}] filed {int(stats['filed']):,}/s (Rs {stats['paise'] / 100:,.0f})  repeats dropped "
                  f"{int(stats['repeats']):,}  behind the newest by {stats['lag']:.2f} s  oldest payment "
                  f"{stats['oldest']:.1f} min old  merchants with late credits {len(seq):,}  total filed "
                  f"{total['filed']:,} repeats {total['repeats']:,}", flush=True)
            stats.clear()

    await asyncio.gather(read(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="passbook")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--poll-ms", type=float, default=20, help="wait between rounds when nothing more is waiting")
    asyncio.run(main(p.parse_args()))
