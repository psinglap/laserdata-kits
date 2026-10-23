"""The Continue Watching service, fed by the heartbeat lane: where every viewer stopped, per title.

Keeps a resume point per viewer and title (viewer as the partition key, title as the clustering
key, like a Continue Watching row), from the newest heartbeat by per-viewer sequence. When a
viewer opens the title on another device, a resume request arrives on resume-requests; the service
answers on resume-answers with the position and the sequence of the heartbeat it came from, so
viewers.py can check it against the truth.

Prints heartbeat-to-resume-point latency every second (from the player stamping the heartbeat to
this service having applied it) with PASS or FAIL against --budget-ms. On start it rebuilds every
resume point by replaying the retained heartbeats, so it holds no state of its own; replayed
heartbeats are not counted as live latency.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, HEARTBEATS, RESUME_ANS, RESUME_REQ, STREAM, Window, load, msg, part, read_partitions

ANS_PARTITIONS = 8


async def main(a):
    async def connected():
        # One connection per reader and writer, so a busy one never queues behind another.
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await connected()
    while not all([await admin.get_topic(STREAM, t) for t in (HEARTBEATS, RESUME_REQ)]):
        print("[continue-watching] waiting for viewers.py to create the topics…", flush=True)
        await asyncio.sleep(1)
    out = await (await connected()).producer(STREAM, RESUME_ANS, topic_partitions_count=ANS_PARTITIONS)
    beats = await connected()                  # the lane: direct partition reads, rebuilt from the first message
    beat_parts = range((await admin.get_topic(STREAM, HEARTBEATS)).partitions_count)
    reqs = await (await connected()).consumer_group(
        "continue-watching-requests", STREAM, RESUME_REQ, polling_strategy=PollingStrategy.Next(),
        batch_length=1000, auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))

    last_seq = {}                              # viewer -> newest heartbeat sequence applied
    resume = defaultdict(dict)                 # viewer -> {title: (seq, position in seconds)}
    buf = []
    lat, stats = Window(), defaultdict(float)
    to_broker, from_broker = Window(), Window()     # where the time goes (--why)
    caught_up = [False]                        # a new reader first replays heartbeats sent before it joined
    gc.freeze()

    async def on_beat(m):
        h = load(m.payload())
        k = h["k"]
        if h["seq"] <= last_seq.get(k, 0):
            return                             # older or repeated heartbeat: the newest position wins
        last_seq[k] = h["seq"]
        resume[k][h["asset"]] = (h["seq"], h["pos"])
        now = time.time_ns()
        age = (now - h["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
            if a.why:
                to_broker.add(m.timestamp() / 1e3 - h["ns"] / 1e6)
                from_broker.add(now / 1e6 - m.timestamp() / 1e3)
        else:                                  # still replaying: track the oldest heartbeat seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_request(m):
        r = load(m.payload())
        seq, pos = resume.get(r["k"], {}).get(r["asset"], (0, None))
        buf.append((r["k"], SendMessage(msg(k=r["k"], rid=r["rid"], asset=r["asset"], pos=pos, seq=seq))))
        stats["answered"] += 1

    async def flush():
        # Every few ms, one batch per partition, sent at once.
        async with out:
            while True:
                if buf:
                    items = buf[:]
                    buf.clear()
                    batches = defaultdict(list)
                    for key, sm in items:
                        batches[part(key, ANS_PARTITIONS)].append(sm)
                    await asyncio.gather(*(out.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                           for pid, ms in batches.items()))
                await asyncio.sleep(0.002)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            if not caught_up[0]:
                # caught up once a whole second of heartbeats arrived inside the budget; partitions interleave,
                # so one fresh heartbeat alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[continue-watching] catching up: replayed {int(stats['replayed']):,} earlier heartbeats "
                      f"this second, viewers with a resume point {len(resume):,}", flush=True)
            else:
                ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
                print(f"[continue-watching] {r['n']:>7,} heartbeats/s  heartbeat-to-resume-point p50 {r['p50']:6.1f}ms  "
                      f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                      f"viewers with a resume point {len(resume):,}  resume requests answered {int(stats['answered']):,}",
                      flush=True)
                if a.why:
                    tb, fb = to_broker.flush(), from_broker.flush()
                    print(f"[why]   heartbeat->broker p50 {tb['p50']:.1f} p99 {tb['p99']:.1f}  "
                          f"broker->service p50 {fb['p50']:.1f} p99 {fb['p99']:.1f} ms", flush=True)
            stats.clear()

    stop = asyncio.Event()
    await asyncio.gather(read_partitions(beats, HEARTBEATS, beat_parts, f"continue-watching-{os.getpid()}", on_beat,
                                         replay=True, poll_ms=0.5),
                         reqs.consume_messages(on_request, stop), flush(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--budget-ms", type=int, default=100)
    p.add_argument("--why", action="store_true", help="also print where the time goes: heartbeat to broker, broker to service")
    asyncio.run(main(p.parse_args()))
