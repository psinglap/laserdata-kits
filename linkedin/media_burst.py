"""A bursty neighbour: media playback events from media-rich Feed scrolling, on their own topic.

LinkedIn's tracking post describes it: "when a member scrolls through a media-rich Feed, render, play, pause, and
exit events arrive close together", and the rebuilt Tracking API isolates categories so that burst cannot take
capacity reserved for other traffic. This sends those four events per video, keyed by member, to media-playback at
--mbps, so you can check that a media burst on the same broker does not move the rt-signals lane's p99 (watch
realtime.py while this runs). One batch per 10 ms tick, spread over partitions by the broker, sized to what the
tick owes, so the neighbour itself does not flood the broker with tiny requests.
"""
import argparse
import asyncio
import gc
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, MEDIA, PARTITIONS, STREAM, ensure_topics, member, msg

STEPS = ("MediaRenderEvent", "MediaPlayEvent", "MediaPauseEvent", "MediaExitEvent")   # illustrative names


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, MEDIA)
    p = await client.producer(STREAM, MEDIA, topic_partitions_count=PARTITIONS[MEDIA],
                              mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(milliseconds=5)))
    pad = os.urandom(max(0, a.size - 160) // 2).hex()     # stands in for the rest of a media event's fields
    per_s = a.mbps * 1e6 / a.size
    if a.at:
        print(f"[media] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    gc.freeze()
    due, sent, t0, end = time.monotonic(), 0, time.monotonic(), time.monotonic() + (a.duration or 1e12)
    owed, last = 0.0, time.monotonic()
    async with p:
        while time.monotonic() < end:
            now = time.monotonic()
            owed += per_s * (now - last)    # what this tick owes, so a slow send is made up in the next batch
            last, n = now, int(owed) // 4 * 4
            owed -= n
            if n:
                ms = []
                for _ in range(n // 4):     # one video in the Feed: render, play, pause, exit, close together
                    mid, video = member(random.randrange(a.members)), f"v{random.getrandbits(40):010x}"
                    ms += [SendMessage(msg(m=mid, t=s, video=video, pos_ms=i * 1500, x=pad)) for i, s in enumerate(STEPS)]
                await p.send_with_partitioning(ms, Partitioning.balanced())
                sent += n
            if time.monotonic() - t0 >= 1:
                print(f"[media] {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{MEDIA}'", flush=True)
                sent, t0 = 0, time.monotonic()
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[media] done", flush=True)      # every batch is flushed: the producer's `async with` waited for it


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=50, help="megabytes per second")
    ap.add_argument("--size", type=int, default=400, help="bytes per media event")
    ap.add_argument("--members", type=int, default=1_000_000)
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
    # Leave without the interpreter's teardown: dropping the SDK client's runtime there could abort the
    # process with a core dump on a CI runner.
    os._exit(0)
