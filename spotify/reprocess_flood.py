"""A heavy neighbour on the same broker: the reprocessing batch job's segment log. Re-processing existing episodes
for playback changes writes a record per transcoded segment (episode, rendition, bitrate, bytes, timings),
bulk data that nobody needs within the second, on its own topic.

Run it while the pipeline and the status service run and watch whether the status lane's p99 moves. One batch
per 10 ms tick with balanced partitioning, as a real high-volume producer batches; one tiny send per partition
per tick would load the broker with requests rather than bytes.
"""
import argparse
import asyncio
import gc
import os
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, REPROCESS, STREAM, connect, ensure_topics, msg


async def main(a):
    client = await connect(a.url)
    await ensure_topics(client, REPROCESS)
    blob = os.urandom(a.size // 2).hex()       # stands in for the segment's encoder settings and timings
    per_tick = max(1, int(a.mbps * 1e6 / a.size / 100))
    if a.at:
        print(f"[reprocess] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    due, sent, t0, end = time.monotonic(), 0, time.monotonic(), time.monotonic() + (a.duration or 1e12)
    while time.monotonic() < end:
        ms = [SendMessage(msg(ep=random.randrange(5_000_000), rendition=random.choice(("360p", "480p", "720p", "1080p", "audio")),
                              seg=random.randrange(2_000), v=blob)) for _ in range(per_tick)]
        await client.send_messages(STREAM, REPROCESS, Partitioning.balanced(), ms)
        sent += per_tick
        if time.monotonic() - t0 >= 1:
            print(f"[reprocess] {sent:,} segment records/s  ~{sent * a.size / 1e6:.1f} MB/s on '{REPROCESS}'", flush=True)
            sent, t0 = 0, time.monotonic()
        due += 0.01
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[reprocess] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=1024, help="bytes per segment record")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
