"""The bulk neighbour next door: a guild's message history backfilled into a search-index topic on the
same broker.

Discord's search post describes reindexing its largest guilds by dual-indexing new messages and
backfilling history into new indices. This script writes that kind of traffic (one message document per
record, padded to --size bytes like a message body plus its index metadata) to a messages topic in its
own stream, in large batches. Run it beside the action lane and watch rules_worker.py: a backfill on the
same broker should not move the action lane's p99.
"""
import argparse
import asyncio
import gc
import os
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, INDEX_STREAM, connect, ensure_topic


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, INDEX_STREAM, "messages", a.partitions)
    body = os.urandom(a.size // 2).hex().encode()       # stands in for the message content and index fields
    per_tick = max(1, int(a.rate * a.tick_ms / 1000))
    guild = 1_000_000_000_000_000_000 + random.randrange(10**15)
    sent, mark, tick, mid = 0, time.monotonic(), time.monotonic(), 0
    start = time.monotonic()
    gc.freeze()
    while True:
        if time.monotonic() - start >= a.at:
            head = b'{"ns":%d,"guild_id":"%d",' % (time.time_ns(), guild)
            ms = []
            for _ in range(per_tick):
                mid += 1
                ms.append(SendMessage(head + b'"message_id":"%d","doc":"' % mid + body + b'"}'))
            # One large batch per tick, round-robin over partitions: no per-key order to keep here.
            await c.send_messages(INDEX_STREAM, "messages", Partitioning.balanced(), ms)
            sent += per_tick
        if time.monotonic() - mark >= 1:
            if time.monotonic() - start >= a.at:
                print(f"[backfill] {sent:,} message documents/s  ~{sent * a.size / 1e6:.1f} MB/s on "
                      f"{INDEX_STREAM}/messages", flush=True)
            sent, mark = 0, time.monotonic()
        tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)
        await asyncio.sleep(max(0.0, tick - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--rate", type=float, default=40_000, help="message documents a second")
    ap.add_argument("--size", type=int, default=800, help="bytes per document")
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--at", type=float, default=0, help="seconds after start the backfill begins")
    ap.add_argument("--tick-ms", type=float, default=20)
    asyncio.run(main(ap.parse_args()))
