"""A heavy neighbour on the same broker: periodic session records (who is watching what, on which device, with
the ad context), the bulk data analytics and reporting read, on their own topic.

Run it while the stitcher and trackers run and watch whether the tracker lane's p99 moves. One batch per 10 ms
tick with balanced partitioning, as a real high-volume producer batches; one tiny send per partition per tick
would load the broker with requests rather than bytes.
"""
import argparse
import asyncio
import gc
import os
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, SESSIONS, STREAM, channel, connect, ensure_topics, msg, session


async def main(a):
    client = await connect(a.url)
    await ensure_topics(client, SESSIONS)
    blob = os.urandom(a.size // 2).hex()       # stands in for device, player, geo and ad context
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 100))
    if a.at:
        print(f"[sessions] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    due, sent, t0, end = time.monotonic(), 0, time.monotonic(), time.monotonic() + (a.duration or 1e12)
    while time.monotonic() < end:
        ms = [SendMessage(msg(s=session(random.randrange(100_000)), ch=channel(random.randrange(50)), v=blob))
              for _ in range(per_tick)]
        await client.send_messages(STREAM, SESSIONS, Partitioning.balanced(), ms)
        sent += per_tick
        if time.monotonic() - t0 >= 1:
            print(f"[sessions] {sent:,} records/s  ~{sent * a.size / 1e6:.1f} MB/s on '{SESSIONS}'", flush=True)
            sent, t0 = 0, time.monotonic()
        due += 0.01
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[sessions] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1024, help="bytes per session record")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
