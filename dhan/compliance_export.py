"""A heavy neighbour: trade records exported in bulk on their own topic (the records behind contract notes and
exchange and regulatory reporting), the shape of a high-volume, latency-tolerant stream on the same broker.

Use it to check that a bulk export next door does not move the order update lane's p99 (watch order_gateway.py
while this runs). One batch per 10 ms tick, spread over partitions by the broker, sized to what the tick owes, so
the neighbour itself does not flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, EXPORT, INSTRUMENTS, STREAM, connect, ensure_topics, msg


async def main(a):
    c = await connect(a.url)
    await ensure_topics(c, EXPORT)
    blob = os.urandom(a.size // 2).hex()       # stands in for the record's detail (charges, taxes, references)
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[compliance-export] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    due, sent, t0, end = time.monotonic(), 0, time.monotonic(), time.monotonic() + (a.duration or 1e12)
    owed, last = 0.0, time.monotonic()
    while time.monotonic() < end:
        now = time.monotonic()
        owed += per_s * (now - last)        # what this tick owes, so a slow send is made up in the next batch
        last, n = now, int(owed)
        owed -= n
        if n:
            ms = [SendMessage(msg(trade_no=f"T{random.randrange(10**12):012d}",
                                  segment=(i := random.choice(INSTRUMENTS))[0], symbol=i[1],
                                  record=random.choice(("contract_note", "trade_file", "margin_report", "stt")),
                                  body=blob)) for _ in range(n)]
            await c.send_messages(STREAM, EXPORT, Partitioning.balanced(), ms)
            sent += n
        if time.monotonic() - t0 >= 1:
            print(f"[compliance-export] {sent:,} records/s  ~{sent * a.size / 1e6:.1f} MB/s on '{EXPORT}'", flush=True)
            sent, t0 = 0, time.monotonic()
        due += 0.01
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[compliance-export] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=2048, help="bytes per record")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
