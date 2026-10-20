"""A fleet of speech engine servers serving live streaming transcription, with every stream's usage on the lane.

Plays --servers inference servers in one process. Together they hold --streams live WebSocket streams
(the /v1/listen streaming method), each owned by a project and opened with one of its API keys, on one model.
Every --interval-s, each live stream publishes a usage message to the usage topic, keyed by project:
  * server, stream (request) ID, project, API key, model, a per-stream sequence, and the stream's billable
    audio so far in ms (the resulting total, never only the last chunk), plus the chunk itself;
  * fin=1 on the stream's last message, when the caller closes it.
Streams last --stream-s on average (exponential), and a closed stream is replaced by a new one.

At-least-once on purpose: --resend-pct of messages are sent a second time, as a server that timed out
waiting for an acknowledgement would. The meter (meter.py) must count each one once.

Prints usage messages, live streams, opened and closed streams and audio served per second.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (DEFAULT_URL, MODEL_MIX, PARTITIONS, STREAM, USAGE, accessor, ensure_topics, msg, part,
                    project)

TICK = 0.01


async def main(a):
    client = IggyClient.from_connection_string(a.url)      # one connection for this producer
    await client.connect()
    await ensure_topics(client, USAGE)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await client.producer(STREAM, USAGE, topic_partitions_count=PARTITIONS[USAGE], mode=cfg)
    servers = [f"engine-{i:02d}" for i in range(a.servers)]
    slots = max(1, round(a.interval_s / TICK))
    due = defaultdict(list)                 # slot -> streams that report in that slot of every interval
    live = [0]
    opened = [0]
    recent = deque(maxlen=4000)             # recently sent usage, for the at-least-once resends
    stats = defaultdict(float)

    def open_stream():
        # A skewed project mix: a few large projects carry much of the audio, many small ones a little.
        pj = project(int(a.projects * random.random() ** 2))
        opened[0] += 1
        st = {"id": f"{opened[0]:08x}-{random.getrandbits(32):08x}", "s": random.choice(servers), "pj": pj,
              "k": accessor(pj, random.randrange(3)), "m": random.choice(MODEL_MIX), "seq": 0, "ms": 0,
              "end": time.monotonic() + random.expovariate(1 / a.stream_s), "q": part(pj, PARTITIONS[USAGE])}
        due[random.randrange(slots)].append(st)
        live[0] += 1
        stats["opened"] += 1

    for _ in range(a.streams):
        open_stream()
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def serve():
        t0 = time.monotonic()
        nxt, tick = t0, 0
        async with producer:
            while True:
                t = time.monotonic() - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                stats["peak"] = x > 1
                target = int(a.streams * x)
                for _ in range(min(max(0, target - live[0]), max(1, target // 200))):
                    open_stream()           # ramp new calls in over a couple of seconds, like a real surge
                now = time.monotonic()
                batches = defaultdict(list)
                keep = []
                for st in due.pop(tick % slots, []):     # streams opened meanwhile land in their own slot
                    chunk = round(a.interval_s * 1000 * random.uniform(0.97, 1.03))   # real-time audio this interval
                    st["seq"] += 1
                    st["ms"] += chunk
                    fin = now >= st["end"] or (live[0] > target and random.random() < 0.02)
                    b = msg(s=st["s"], id=st["id"], pj=st["pj"], k=st["k"], m=st["m"], seq=st["seq"],
                            ms=st["ms"], d=chunk, fin=int(fin))
                    batches[st["q"]].append(SendMessage(b))
                    stats["sent"] += 1
                    stats["audio_ms"] += chunk
                    if a.resend_pct and random.random() < a.resend_pct / 100 and recent:
                        q, old = random.choice(recent)    # a retry of an earlier message: same stream, same seq
                        batches[q].append(SendMessage(old))
                        stats["resent"] += 1
                    recent.append((st["q"], b))
                    if fin:
                        live[0] -= 1
                        stats["closed"] += 1
                        if live[0] < target:
                            open_stream()
                    else:
                        keep.append(st)
                due[tick % slots].extend(keep)
                # One batch per partition, all sent at once: a project's usage stays in order, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                tick += 1
                nxt += TICK
                await asyncio.sleep(max(0.0, nxt - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "peak" if stats["peak"] else "steady"
            print(f"[engine] {int(stats['sent']):>7,} usage msgs/s ({phase})  live streams {live[0]:,} on "
                  f"{a.servers} servers  opened {int(stats['opened']):,}/s closed {int(stats['closed']):,}/s  "
                  f"audio served {stats['audio_ms'] / 1000:,.0f} s/s  resent {int(stats['resent']):,}", flush=True)
            for k in ("sent", "opened", "closed", "audio_ms", "resent"):
                stats[k] = 0

    await asyncio.gather(serve(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--servers", type=int, default=8, help="inference servers")
    p.add_argument("--streams", type=int, default=20_000, help="live streams across the fleet")
    p.add_argument("--projects", type=int, default=5_000, help="projects the streams belong to")
    p.add_argument("--interval-s", type=float, default=1.0, help="each live stream reports its usage this often")
    p.add_argument("--stream-s", type=float, default=90, help="mean stream length in seconds")
    p.add_argument("--resend-pct", type=float, default=0.5, help="share of usage messages sent twice (retries)")
    p.add_argument("--peak", type=float, default=1, help="live-stream multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
